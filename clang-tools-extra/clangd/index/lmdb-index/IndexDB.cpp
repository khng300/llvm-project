//===--- IndexDB.cpp - Symbol database --------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "IndexDB.h"

namespace clang {
namespace clangd {
namespace lmdb_index {
namespace {

std::string IndexErrorString[static_cast<size_t>(IndexDBError::Last)] = {
    "Success",
    "Object not found",
    "Invalid cursor",
    "Indexing layout mismatch",
    "Unsupported layout",
    "Corrupted",
    "Internal error",
    "DB Inconsistency",
};

IndexDBErrorCategory IndexErrCategory;

} // namespace

const char *IndexDBErrorCategory::name() const noexcept {
  return "indexdb.error";
}

std::string IndexDBErrorCategory::message(int Condition) const {
  if (Condition >= static_cast<int>(IndexDBError::Last))
    return "???";
  return IndexErrorString[Condition];
}

// NOLINTBEGIN(readability-identifier-naming)
std::error_code make_error_code(IndexDBError E) {
  return std::error_code(static_cast<int>(E), IndexErrCategory);
}
// NOLINTEND(readability-identifier-naming)

std::error_code mapDBErrorToIndexDBError(std::error_code EC) {
  if (EC.category() == IndexErrCategory)
    return EC;
  if (EC == lmdb::DBError::Notfound)
    return IndexDBError::Notfound;
  return EC;
}

llvm::ErrorOr<OID> DatabaseBase::numberOfEntries(lmdb::Txn &Txn) {
  auto Stat = Objects.stat(Txn);
  if (!Stat)
    return IndexDBError::InternalError;
  return Stat->ms_entries;
}

llvm::ErrorOr<std::optional<OID>> DatabaseBase::lastOid(lmdb::Txn &Txn) {
  auto Cur = lmdb::Cursor::open(Txn, Objects);
  if (!Cur)
    return Cur.getError();
  Slice ValOid;
  auto EC = Cur->get(ValOid, MDB_LAST);
  if (EC) {
    if (EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
    return std::nullopt;
  }
  return ValOid.toTypeRef<OID>();
}

std::error_code DatabaseBase::open(lmdb::Txn &Txn) {
  std::string OidName = NamePrefix + "_OID";
  std::string FreedOidName = NamePrefix + "_FREED_OID";
  auto DBIOrErr =
      lmdb::DBI::open(Txn, OidName.c_str(), MDB_INTEGERKEY | MDB_CREATE);
  if (!DBIOrErr)
    return mapDBErrorToIndexDBError(DBIOrErr.getError());
  Objects = std::move(*DBIOrErr);
  DBIOrErr =
      lmdb::DBI::open(Txn, FreedOidName.c_str(), MDB_INTEGERKEY | MDB_CREATE);
  if (!DBIOrErr)
    return mapDBErrorToIndexDBError(DBIOrErr.getError());
  FreedOids = std::move(*DBIOrErr);
  for (uint32_t J = 0; J < NumOfIndexColumns; J++) {
    std::string DBName = NamePrefix + "_KEYS_OID_" + std::to_string(J);
    DBIOrErr = lmdb::DBI::open(Txn, DBName.c_str(),
                               IndexColumnSettings[J] | MDB_DUPSORT |
                                   MDB_INTEGERDUP | MDB_DUPFIXED | MDB_CREATE);
    if (!DBIOrErr)
      return mapDBErrorToIndexDBError(DBIOrErr.getError());
    IndexOids[J] = std::move(*DBIOrErr);
  }
  return {};
}

std::error_code DatabaseBase::insertOidToFreetable(lmdb::Txn &Txn, OID Oid) {
  auto EC = Objects.del(Txn, makeSlice(Oid));
  if (EC && EC != lmdb::DBError::Notfound)
    return mapDBErrorToIndexDBError(EC);
  Slice Val;
  EC = FreedOids.put(Txn, makeSlice(Oid), Val);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  return {};
}

llvm::ErrorOr<OID> DatabaseBase::allocOid(lmdb::Txn &Txn, Slice ObjSlice) {
  llvm::ErrorOr<lmdb::Cursor> Cur = lmdb::Cursor::open(Txn, FreedOids);
  if (!Cur)
    return mapDBErrorToIndexDBError(Cur.getError());
  llvm::ErrorOr<OID> OidOrErr = [&]() -> llvm::ErrorOr<OID> {
    Slice ValOid;
    auto EC = Cur->get(ValOid, MDB_NEXT);
    if (EC && EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
    if (EC) {
      auto StatOrErr = Objects.stat(Txn);
      if (!StatOrErr)
        return mapDBErrorToIndexDBError(StatOrErr.getError());
      return StatOrErr->ms_entries + 1;
    }
    OID Ret = ValOid.toTypeRef<OID>();
    EC = Cur->del();
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    return Ret;
  }();
  if (!OidOrErr)
    return OidOrErr.getError();
  OID Oid = *OidOrErr;
  auto EC = Objects.put(Txn, makeSlice(Oid), ObjSlice);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  EC = insertIndexToOid(Txn, Oid, ObjSlice);
  if (EC)
    return EC;
  return Oid;
}

std::error_code DatabaseBase::removeOid(lmdb::Txn &Txn, OID Oid) {
  Slice ObjSlice;
  auto EC = Objects.get(Txn, makeSlice(Oid), ObjSlice);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  EC = removeIndexToOid(Txn, Oid, ObjSlice);
  if (EC)
    return EC;
  EC = Objects.del(Txn, makeSlice(Oid));
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  return insertOidToFreetable(Txn, Oid);
}

std::error_code DatabaseBase::writeOid(lmdb::Txn &Txn, OID Oid,
                                       Slice ObjSlice) {
  IndexSet OldIndexes, NewIndexes;
  auto EC = [&]() -> std::error_code {
    Slice OldObjSlice;
    auto EC = Objects.get(Txn, makeSlice(Oid), OldObjSlice);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    if (OldObjSlice.size() != 0) {
      index(OldObjSlice, [&](IndexID IndexId, Slice D) {
        OldIndexes.emplace(IndexId, D.vec());
        return true;
      });
    }
    return {};
  }();
  if (EC)
    return EC;
  if (ObjSlice.size() != 0) {
    index(ObjSlice, [&](IndexID IndexId, Slice D) {
      NewIndexes.emplace(IndexId, D.vec());
      return true;
    });
  }
  EC = Objects.put(Txn, makeSlice(Oid), ObjSlice, 0);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  return updateIndexesToOid(Txn, Oid, OldIndexes, NewIndexes);
}

llvm::ErrorOr<IndexOIDCursor>
DatabaseBase::findOid(lmdb::Txn &Txn, IndexID IndexId, Slice Key) {
  llvm::ErrorOr<lmdb::Cursor> Cur = lmdb::Cursor::open(Txn, IndexOids[IndexId]);
  if (!Cur)
    return mapDBErrorToIndexDBError(Cur.getError());
  auto EC = Cur->get(Key, MDB_SET);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  return IndexOIDCursor(std::move(*Cur));
}

llvm::ErrorOr<Slice> DatabaseBase::getOid(lmdb::Txn &Txn, OID Oid) {
  Slice ObjSlice;
  auto EC = Objects.get(Txn, makeSlice(Oid), ObjSlice);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  return ObjSlice;
}

std::error_code DatabaseBase::findAllOfOidsByKey(
    lmdb::Txn &Txn, IndexID IndexId, Slice Key,
    llvm::function_ref<bool(OID, Slice)> Callback) {
  llvm::ErrorOr<lmdb::Cursor> Cur = lmdb::Cursor::open(Txn, IndexOids[IndexId]);
  if (!Cur)
    return mapDBErrorToIndexDBError(Cur.getError());
  Slice Val;
  std::error_code EC;
  for (EC = Cur->get(Key, Val, MDB_SET); !EC;
       EC = Cur->get(Key, Val, MDB_NEXT_DUP)) {
    OID Oid = llvm::bit_cast<OID>(*static_cast<const OID *>(Val.data()));
    llvm::ErrorOr<Slice> ObjSlice = getOid(Txn, Oid);
    if (!ObjSlice)
      return ObjSlice.getError();
    if (!Callback(Oid, *ObjSlice))
      break;
  }
  if (EC && EC != lmdb::DBError::Notfound)
    return mapDBErrorToIndexDBError(EC);
  return {};
}

llvm::ErrorOr<IndexCursor> DatabaseBase::getIndexCursor(lmdb::Txn &Txn,
                                                        IndexID IndexId) {
  llvm::ErrorOr<lmdb::Cursor> Cur = lmdb::Cursor::open(Txn, IndexOids[IndexId]);
  if (!Cur)
    return mapDBErrorToIndexDBError(Cur.getError());
  Slice Key;
  auto EC = Cur->get(Key, MDB_FIRST);
  if (EC)
    return mapDBErrorToIndexDBError(Cur.getError());
  return IndexCursor(std::move(*Cur));
}

llvm::ErrorOr<OIDCursor> DatabaseBase::getOidCursor(lmdb::Txn &Txn) {
  llvm::ErrorOr<lmdb::Cursor> Cur = lmdb::Cursor::open(Txn, Objects);
  if (!Cur)
    return mapDBErrorToIndexDBError(Cur.getError());
  Slice Key;
  auto EC = Cur->get(Key, MDB_FIRST);
  if (EC)
    return mapDBErrorToIndexDBError(Cur.getError());
  return OIDCursor(std::move(*Cur));
}

std::error_code DatabaseBase::insertIndexToOid(lmdb::Txn &Txn, OID Oid,
                                               Slice ObjSlice) {
  std::error_code EC;
  if (ObjSlice.size() != 0) {
    index(ObjSlice, [&](IndexID IndexId, Slice D) {
      Slice Val = makeSlice(Oid);
      EC = mapDBErrorToIndexDBError(IndexOids[IndexId].put(Txn, D, Val));
      return EC ? false : true;
    });
  }
  return EC;
}

std::error_code DatabaseBase::removeIndexToOid(lmdb::Txn &Txn, OID Oid,
                                               Slice ObjSlice) {
  std::error_code EC;
  if (ObjSlice.size() != 0) {
    index(ObjSlice, [&](IndexID IndexId, Slice D) {
      EC = IndexOids[IndexId].del(Txn, D, makeSlice(Oid));
      if (EC == lmdb::DBError::Notfound)
        EC = {};
      EC = mapDBErrorToIndexDBError(EC);
      return EC ? false : true;
    });
  }
  return EC;
}

std::error_code DatabaseBase::updateIndexesToOid(lmdb::Txn &Txn, OID Oid,
                                                 IndexSet &OldIndexes,
                                                 IndexSet &NewIndexes) {
  // No need to work on unchanged indexes.
  for (auto It = OldIndexes.begin(), NextIt = OldIndexes.end();
       (It != OldIndexes.end() ? (NextIt = std::next(It), true) : false);
       It = NextIt) {
    if (auto NewIt = NewIndexes.find(*It); NewIt != NewIndexes.end()) {
      NewIndexes.erase(NewIt);
      OldIndexes.erase(It);
    }
  }

  // The first pass removes stale indexes.
  Slice Val = makeSlice(Oid);
  for (const auto &I : OldIndexes) {
    const auto &[IndexId, D] = I;
    auto EC = mapDBErrorToIndexDBError(IndexOids[IndexId].del(Txn, D, Val));
    if (EC && EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
  }
  // The second pass inserts new indexes.
  for (const auto &I : NewIndexes) {
    const auto &[IndexId, D] = I;
    auto EC = mapDBErrorToIndexDBError(IndexOids[IndexId].put(Txn, D, Val));
    if (EC)
      return mapDBErrorToIndexDBError(EC);
  }
  return {};
}

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
