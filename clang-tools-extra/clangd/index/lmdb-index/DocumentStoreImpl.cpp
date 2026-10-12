//===--- DocumentStoreImpl.cpp - Symbol database ----------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "DocumentStoreImpl.h"
#include "Merge.h"
#include "support/Logger.h"
#include "llvm/Support/FileSystem.h"

#include <optional>
#include <system_error>

using namespace clang::clangd::lmdb_index::internal;

namespace clang {
namespace clangd {
namespace lmdb_index {

namespace {

std::string DSErrorString[static_cast<size_t>(DSError::Last)] = {
    "Success",           "Object not found",
    "Database map full", "Unsupported layout",
    "Internal Error",    "DB Inconsistency",
};

DSErrorCategory DSErrCategory;

} // namespace

namespace internal {
std::error_code mapErrorCodeCommon(std::error_code EC) {
  if (EC.category() == DSErrCategory)
    return EC;
  if (EC == lmdb::DBError::Notfound)
    return DSError::Notfound;
  if (EC == lmdb::DBError::MapFull)
    return DSError::DatabaseMapFull;
  return EC;
}
} // namespace internal

const char *DSErrorCategory::name() const noexcept { return "indexdb.error"; }

std::string DSErrorCategory::message(int Condition) const {
  if (Condition >= static_cast<int>(DSError::Last))
    return "???";
  return DSErrorString[Condition];
}

// NOLINTBEGIN(readability-identifier-naming)
std::error_code make_error_code(DSError E) {
  return std::error_code(static_cast<int>(E), DSErrCategory);
}
// NOLINTEND(readability-identifier-naming)

std::optional<std::pair<OID, const cl::IndexDataFile *>>
TransactionImpl::KeyToIDFilesIterator::opImpl(MDB_cursor_op Op) {
  assert(!ErrorCode);
  auto OidSlice = Base::opImpl(Op);
  if (!OidSlice) {
    if (auto EC = Base::errorCode()) {
      if (EC == lmdb::DBError::Notfound)
        ErrorCode = DSError::Notfound;
      else
        ErrorCode = EC;
    }
    return std::nullopt;
  }
  OID Oid = OidSlice->toTypeRef<OID>();
  auto IDFileOrErr = Txn->getIndexDataFile(Oid);
  if (!IDFileOrErr) {
    ErrorCode = IDFileOrErr.getError();
    return {};
  }
  return std::pair{Oid, *IDFileOrErr};
}

OID TransactionImpl::getNewOid() {
  union {
    struct {
      uint64_t TxnId;
      uint64_t Counter;
    } U;
    uint8_t Bytes[16];
  } OidDraft = {{Txn.txnID(), AllocationCounter++}};
  return llvm::xxh3_64bits(OidDraft.Bytes);
}

llvm::ErrorOr<std::unique_ptr<TransactionImpl>>
TransactionImpl::begin(DocumentStoreImpl &DB, bool RO, llvm::StringRef Tag) {
  EnvResizeLock EnvResizeLk;
  llvm::ErrorOr<lmdb::Txn> TxnOrErr{std::error_code()};
  do {
    EnvResizeLk = std::shared_lock<std::shared_mutex>(DB.EnvResizeRWMu);
    TxnOrErr = lmdb::Txn::begin(DB.Env, RO ? MDB_RDONLY : 0);
    if (!TxnOrErr) {
      auto EC = TxnOrErr.getError();
      if (EC != lmdb::DBError::MapResized)
        return mapErrorCodeCommon(EC);
      // Refresh the map size as it was changed outside our process.
      EnvResizeLk = std::unique_lock<std::shared_mutex>(DB.EnvResizeRWMu);
      DB.Env.setMapSize(0);
    }
  } while (!TxnOrErr);
  return std::unique_ptr<TransactionImpl>(new TransactionImpl(
      DB, std::move(EnvResizeLk), std::move(*TxnOrErr), Tag));
}

size_t TransactionImpl::databaseMapSize() {
  return DB->Env.getEnvInfo().me_mapsize;
}

std::error_code TransactionImpl::commit() && {
  auto StartCommitTime = std::chrono::system_clock::now();
  auto EC = Txn.commit();
  if (EC)
    return mapErrorCodeCommon(EC);
  auto EndCommitTime = std::chrono::system_clock::now();

  log("commit(): Tag = {0}:\n"
      "\tIndexDuration = {1}\n"
      "\tDatafileDuration = {2}\n"
      "\tUpdatedSymbolsDuration = {3}\n"
      "\tCommitDuration = {4}",
      Tag, IndexDuration, DatafileDuration, UpdatedSymbolsDuration,
      EndCommitTime - StartCommitTime);
  return {};
}

void TransactionImpl::abort() && { Txn.abort(); }

void TransactionImpl::raiseFatalError(std::error_code EC) {
  DB->raiseFatalError(EC);
}

llvm::ErrorOr<OID>
TransactionImpl::createEmptyIndexDataFile(llvm::StringRef ShardPath,
                                          llvm::StringRef FileURI) {
  cl::IndexDataFile IDFile = [&]() {
    cl::IndexDataFile Ret;
    Ret.ShardPath = ShardPath;
    Ret.FileURI = FileURI;
    Ret.ShardPathHash = generateObjectHash(ShardPath);
    Ret.FileURIHash = generateObjectHash(FileURI);
    return Ret;
  }();
  return createIndexDataFile(IDFile);
}

llvm::ErrorOr<cl::IndexDataFile>
TransactionImpl::toIndexDataFile(llvm::StringRef ShardPath,
                                 const IndexFileOut &Shard,
                                 int64_t ModifiedTime) {
  cl::IndexDataFile Ret;
  URI Uri = URI::createFile(ShardPath);
  Ret.ShardPath = ShardPath;
  Ret.FileURI = Uri.toString();
  Ret.ShardPathHash = generateObjectHash(ShardPath);
  Ret.FileURIHash = generateObjectHash(Ret.FileURI.view());
  Ret.ModifiedTime = ModifiedTime;
  Ret.Valid = true;
  if (Shard.Cmd)
    Ret.Cmd = Marshaller.fromNative(*Shard.Cmd);
  if (Shard.Sources)
    Ret.Sources = Marshaller.fromNative(*Shard.Sources);
  if (Shard.Symbols) {
    for (const auto &Sym : *Shard.Symbols) {
      auto Res = Marshaller.fromNative(Sym);
      if (!Res)
        return Res.getError();
      Ret.Symbols[Res->ID] = *Res;
    }
  }
  if (Shard.Refs) {
    for (const auto &RefList : *Shard.Refs) {
      for (const auto &Ref : RefList.second) {
        auto Res = Marshaller.fromNative(Ref);
        if (!Res)
          return Res.getError();
        Ret.Refs[Marshaller.fromNative(RefList.first)].emplace_back(*Res);
        if (Ref.Kind == RefKind::Unknown)
          continue;
        auto Res2 = Marshaller.fromNative(
            ContainedRefsResult{Ref.Location, Ref.Kind, RefList.first});
        if (!Res2)
          return Res2.getError();
        Ret.RevRefs[Res->Container].emplace_back(*Res2);
      }
    }
  }
  if (Shard.Relations) {
    for (const auto &Relation : *Shard.Relations) {
      auto Res = Marshaller.fromNative(Relation);
      Ret.Relations[{Res.Subject, Res.Predicate}].emplace_back(Res);
    }
  }
  return Ret;
}

llvm::ErrorOr<OID>
TransactionImpl::createIndexDataFile(const cl::IndexDataFile &IDFile) {
  OID Oid;
  std::error_code EC;
  auto SerializedIDFile = serializeObject(IDFile);
  Slice OidSlice, IDFileSlice = SerializedIDFile;
  do {
    Oid = getNewOid();
    OidSlice = makeSlice(Oid);
    EC = DB->IndexDataFilesDbi.put(Txn, OidSlice, IDFileSlice, MDB_NOOVERWRITE);
    if (EC && EC != lmdb::DBError::Keyexist)
      return mapErrorCodeCommon(EC);
  } while (EC == lmdb::DBError::Keyexist);
  Slice HashedURISlice = makeSlice(IDFile.FileURIHash),
        HashedPathSlice = makeSlice(IDFile.ShardPathHash);
  EC = DB->HashedURIToIDFileDbi.put(Txn, HashedURISlice, OidSlice);
  if (EC)
    return mapErrorCodeCommon(EC);
  EC = DB->HashedPathToIDFileDbi.put(Txn, HashedPathSlice, OidSlice);
  if (EC)
    return mapErrorCodeCommon(EC);
  return Oid;
}

llvm::ErrorOr<const cl::IndexDataFile *>
TransactionImpl::getIndexDataFile(OID Oid) {
  Slice IDFileSlice;
  auto EC = DB->IndexDataFilesDbi.get(Txn, makeSlice(Oid), IDFileSlice);
  if (EC)
    return mapErrorCodeCommon(EC);
  return &IDFileSlice.toTypeRef<cl::IndexDataFile>();
}

llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
TransactionImpl::getIndexDataFileByShardPath(llvm::StringRef ShardPath) {
  auto HashedPath = generateObjectHash(ShardPath);
  auto It =
      getKeyToFileDbiIterator(DB->HashedPathToIDFileDbi, makeSlice(HashedPath));
  if (!It)
    return It.getError();
  for (auto Res = It->peek(); Res; Res = It->next()) {
    const auto &IDFile = *Res->second;
    if (IDFile.ShardPath == ShardPath)
      return std::pair{Res->first, &IDFile};
  }
  if (auto EC = It->errorCode())
    return EC;
  return DSError::Notfound;
}

llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
TransactionImpl::getIndexDataFileByURI(llvm::StringRef FileURI) {
  auto HashedURI = generateObjectHash(FileURI);
  auto It =
      getKeyToFileDbiIterator(DB->HashedURIToIDFileDbi, makeSlice(HashedURI));
  if (!It)
    return It.getError();
  for (auto Res = It->peek(); Res; Res = It->next()) {
    const auto &IDFile = *Res->second;
    if (IDFile.FileURI == FileURI)
      return std::pair{Res->first, &IDFile};
  }
  if (auto EC = It->errorCode())
    return EC;
  return DSError::Notfound;
}

std::error_code TransactionImpl::removeIndexDataFile(OID Oid) {
  Slice IDFileSlice, OidSlice = makeSlice(Oid);
  auto EC = DB->IndexDataFilesDbi.get(Txn, OidSlice, IDFileSlice);
  if (EC)
    return mapErrorCodeCommon(EC);
  {
    const auto &IDFile = IDFileSlice.toTypeRef<cl::IndexDataFile>();
    EC = DB->HashedURIToIDFileDbi.del(Txn, makeSlice(IDFile.FileURIHash),
                                      OidSlice);
    if (EC)
      return mapErrorCodeCommon(EC);
    EC = DB->HashedPathToIDFileDbi.del(Txn, makeSlice(IDFile.ShardPathHash),
                                       OidSlice);
    if (EC)
      return mapErrorCodeCommon(EC);
  }
  EC = DB->IndexDataFilesDbi.del(Txn, OidSlice);
  if (EC)
    return mapErrorCodeCommon(EC);
  return {};
}

std::error_code
TransactionImpl::writeIndexDataFile(OID Oid, const cl::IndexDataFile &IDFile) {
  auto SerializedIDFile = serializeObject(IDFile);
  Slice IDFileSlice = SerializedIDFile;
  if (auto EC = DB->IndexDataFilesDbi.put(Txn, makeSlice(Oid), IDFileSlice))
    return mapErrorCodeCommon(EC);
  return {};
}

std::unique_ptr<IndexFileInfo>
TransactionImpl::loadShardInfo(llvm::StringRef ShardPath) {
  auto Res = getIndexDataFileByShardPath(ShardPath);
  if (!Res || !Res->second->Valid)
    return nullptr;
  const cl::IndexDataFile &IDFile = *Res->second;
  IndexFileInfo IFileInfo;
  IFileInfo.FileURI = IDFile.FileURI;
  IFileInfo.ShardPath = IDFile.ShardPath;
  IFileInfo.ModifiedTime = IDFile.ModifiedTime;
  if (IDFile.Sources)
    IFileInfo.Sources = Marshaller.toNative(*IDFile.Sources);
  if (IDFile.Cmd)
    IFileInfo.Cmd = Marshaller.toNative(*IDFile.Cmd);
  return std::make_unique<IndexFileInfo>(std::move(IFileInfo));
}

llvm::ErrorOr<OID>
TransactionImpl::ensureShardExistence(llvm::StringRef FileURI) {
  auto Res = getIndexDataFileByURI(FileURI);
  if (!Res) {
    auto PathOrErr = URI::resolve(FileURI);
    if (!PathOrErr) {
      elog("ensureShardExistence: Invalid FileURI encountered: {0}", FileURI);
      return DSError::InternalError;
    }
    return createEmptyIndexDataFile(*PathOrErr, FileURI);
  }
  return Res->first;
}

llvm::ErrorOr<cl::Symbol> TransactionImpl::lookupShardsSymbol(SymbolID ID) {
  cl::SymbolID SymId = Marshaller.fromNative(ID);
  auto It =
      getKeyToFileDbiIterator(DB->SymbolIDToIDFileDbi, lmdb::makeSlice(SymId));
  if (!It)
    return It.getError();
  llvm::SmallVector<const cl::Symbol *> Syms;
  for (auto Res = It->peek(); Res; Res = It->next()) {
    const auto &IDFile = *Res->second;
    auto SymIt = IDFile.Symbols.find(SymId);
    if (SymIt == IDFile.Symbols.end())
      return DSError::DBInconsistent;
    Syms.emplace_back(&SymIt->second);
  }
  if (auto EC = It->errorCode())
    return EC;
  assert(!Syms.empty());
  cl::Symbol Sym;
  for (const auto &V : Syms)
    Sym = Sym.ID ? mergeSymbol(Sym, *V) : *V;
  return Sym;
}

llvm::ErrorOr<cl::IndexedSymbol> TransactionImpl::findSymbol(SymbolID ID) {
  Slice OidSlice;
  auto EC = DB->SymbolIDToISymbolDbi.get(Txn, ID.raw(), OidSlice);
  if (EC)
    return mapErrorCodeCommon(EC);
  Slice ISymbolSlice;
  EC = DB->IndexedSymbolsDbi.get(Txn, OidSlice, ISymbolSlice);
  if (EC) {
    if (EC != lmdb::DBError::Notfound)
      return mapErrorCodeCommon(EC);
    return DSError::DBInconsistent;
  }
  return ISymbolSlice.toTypeRef<cl::IndexedSymbol>();
}

llvm::ErrorOr<TransactionImpl::KeyToIDFilesIterator>
TransactionImpl::getKeyToFileDbiIterator(lmdb::DBI &Dbi, Slice Key) {
  auto CurOrErr = lmdb::Cursor::open(Txn, Dbi);
  if (!CurOrErr)
    return mapErrorCodeCommon(CurOrErr.getError());
  auto &Cursor = *CurOrErr;
  auto EC = Cursor.get(Key, MDB_SET);
  if (EC)
    return mapErrorCodeCommon(EC);
  return TransactionImpl::KeyToIDFilesIterator{*this, std::move(Cursor)};
}

std::error_code
TransactionImpl::insertShardIndexes(OID ShardOid,
                                    const cl::IndexDataFile &IDFile) {
  Slice OidSlice = makeSlice(ShardOid);
  auto Inserter = [&](lmdb::DBI &IdxDbi, const auto &Table) -> std::error_code {
    for (const auto &[Key, _] : Table) {
      Slice KeySlice = makeSlice(Key);
      if (auto EC = IdxDbi.put(Txn, KeySlice, OidSlice))
        return mapErrorCodeCommon(EC);
    }
    return {};
  };

  auto EC = Inserter(DB->SymbolIDToIDFileDbi, IDFile.Symbols);
  if (EC)
    return EC;
  EC = Inserter(DB->RefSymbolIDToIDFileDbi, IDFile.Refs);
  if (EC)
    return EC;
  EC = Inserter(DB->RelationKeyToIDFileDbi, IDFile.Relations);
  if (EC)
    return EC;
  EC = Inserter(DB->RevRefSymbolIDToIDFileDbi, IDFile.RevRefs);
  if (EC)
    return EC;
  return {};
}

std::error_code
TransactionImpl::removeShardIndexes(OID ShardOid,
                                    const cl::IndexDataFile &IDFile) {
  Slice OidSlice = makeSlice(ShardOid);
  auto Remover = [&](lmdb::DBI &IdxDbi, const auto &Table) -> std::error_code {
    for (const auto &[Key, _] : Table) {
      Slice KeySlice = makeSlice(Key);
      if (auto EC = IdxDbi.del(Txn, KeySlice, OidSlice)) {
        if (EC != lmdb::DBError::Notfound)
          return mapErrorCodeCommon(EC);
        return DSError::DBInconsistent;
      }
    }
    return {};
  };

  auto EC = Remover(DB->SymbolIDToIDFileDbi, IDFile.Symbols);
  if (EC)
    return EC;
  EC = Remover(DB->RefSymbolIDToIDFileDbi, IDFile.Refs);
  if (EC)
    return EC;
  EC = Remover(DB->RelationKeyToIDFileDbi, IDFile.Relations);
  if (EC)
    return EC;
  EC = Remover(DB->RevRefSymbolIDToIDFileDbi, IDFile.RevRefs);
  if (EC)
    return EC;
  return {};
}

std::error_code TransactionImpl::updateShard(llvm::StringRef ShardPath,
                                             const IndexFileOut *Shard,
                                             int64_t ModifiedTime) {
  trace::Span("LMDB::Txn::updateShard");
  bool RemoveShard = Shard == nullptr;

  auto PrevShard = getIndexDataFileByShardPath(ShardPath);
  if (!PrevShard) {
    // No corresponding old shard, thus the error must be DSError::Notfound
    // or it indicates error.
    if (auto EC = PrevShard.getError(); EC != DSError::Notfound)
      return EC;
  } else {
    // Old shard's data needs to be removed first.
    OID Oid = PrevShard->first;
    auto StartTime = std::chrono::system_clock::now();
    std::error_code EC = removeShardIndexes(Oid, *PrevShard->second);
    if (EC)
      return EC;
    IndexDuration += std::chrono::system_clock::now() - StartTime;

    StartTime = std::chrono::system_clock::now();
    for (const auto &[SymId, _] : PrevShard->second->Symbols) {
      Slice EmptySlice;
      EC = DB->UpdatedSymbolsDbi.put(Txn, makeSlice(SymId), EmptySlice,
                                     MDB_NOOVERWRITE);
      if (EC && EC != lmdb::DBError::Keyexist)
        return mapErrorCodeCommon(EC);
    }
    UpdatedSymbolsDuration += std::chrono::system_clock::now() - StartTime;

    if (RemoveShard) {
      StartTime = std::chrono::system_clock::now();
      EC = removeIndexDataFile(Oid);
      if (EC)
        return EC;
      DatafileDuration += std::chrono::system_clock::now() - StartTime;
    }
  }

  if (!RemoveShard) {
    auto StartTime = std::chrono::system_clock::now();
    llvm::ErrorOr<OID> Oid =
        ensureShardExistence(URI::createFile(ShardPath).toString());
    if (!Oid)
      return Oid.getError();
    auto IDFileOrErr = toIndexDataFile(ShardPath, *Shard, ModifiedTime);
    if (!IDFileOrErr)
      return IDFileOrErr.getError();
    const cl::IndexDataFile &IDFile = *IDFileOrErr;
    auto EC = writeIndexDataFile(*Oid, IDFile);
    if (EC)
      return EC;
    DatafileDuration += std::chrono::system_clock::now() - StartTime;

    StartTime = std::chrono::system_clock::now();
    EC = insertShardIndexes(*Oid, IDFile);
    if (EC)
      return EC;
    IndexDuration += std::chrono::system_clock::now() - StartTime;

    StartTime = std::chrono::system_clock::now();
    for (const auto &[SymId, _] : IDFile.Symbols) {
      Slice EmptySlice;
      EC = DB->UpdatedSymbolsDbi.put(Txn, makeSlice(SymId), EmptySlice,
                                     MDB_NOOVERWRITE);
      if (EC && EC != lmdb::DBError::Keyexist)
        return mapErrorCodeCommon(EC);
    }
    UpdatedSymbolsDuration += std::chrono::system_clock::now() - StartTime;
  }

  return {};
}

std::error_code TransactionImpl::updateSymbol(SymbolID ID) {
  std::optional<OID> ExistingOid;
  Slice OidSlice;
  auto EC = DB->SymbolIDToISymbolDbi.get(Txn, ID.raw(), OidSlice);
  if (EC) {
    if (EC != lmdb::DBError::Notfound)
      return mapErrorCodeCommon(EC);
  } else
    ExistingOid = OidSlice.toTypeRef<OID>();

  auto SerializedSym = lookupShardsSymbol(ID);
  if (!SerializedSym) {
    EC = SerializedSym.getError();
    if (EC != DSError::Notfound)
      return EC;
    if (ExistingOid) {
      //
      // No more Symbols exist in the index data files. Remove the ISymbol if
      // one is found.
      //
      OidSlice = makeSlice(*ExistingOid);
      Slice ExistingISymbol;
      EC = DB->IndexedSymbolsDbi.get(Txn, OidSlice, ExistingISymbol);
      if (EC)
        return mapErrorCodeCommon(EC);
      const auto &ISymbol = ExistingISymbol.toTypeRef<cl::IndexedSymbol>();
      EC = DB->SymbolIDToISymbolDbi.del(Txn, ID.raw(), OidSlice);
      if (EC) {
        if (EC != lmdb::DBError::Notfound)
          return mapErrorCodeCommon(EC);
        return DSError::DBInconsistent;
      }
      for (const auto &V : ISymbol.Tokens) {
        EC = DB->HashedTokenToISymbolDbi.del(Txn, makeSlice(V), OidSlice);
        if (EC) {
          if (EC != lmdb::DBError::Notfound)
            return mapErrorCodeCommon(EC);
          return DSError::DBInconsistent;
        }
      }
      EC = DB->IndexedSymbolsDbi.del(Txn, OidSlice);
      if (EC) {
        if (EC != lmdb::DBError::Notfound)
          return mapErrorCodeCommon(EC);
        return DSError::DBInconsistent;
      }
    }
    return {};
  }
  auto SymOrErr = Marshaller.toNative(*SerializedSym);
  if (!SymOrErr)
    return SymOrErr.getError();
  cl::IndexedSymbol ISymbol = [&]() {
    std::vector<Token> Tokens = buildTokens(*SymOrErr);
    cl::data::vector<uint64_t> HashedTokens;
    HashedTokens.reserve(Tokens.size());
    for (const auto &Tok : Tokens)
      HashedTokens.emplace_back(Tok.getXxh3());
    return cl::IndexedSymbol{*SerializedSym, std::move(HashedTokens)};
  }();
  auto SerializedISymbol = serializeObject(ISymbol);
  if (!ExistingOid) {
    do {
      OID Oid = getNewOid();
      OidSlice = makeSlice(Oid);
      Slice ISymbolSlice = SerializedISymbol;
      EC = DB->IndexedSymbolsDbi.put(Txn, OidSlice, ISymbolSlice,
                                     MDB_NOOVERWRITE);
      if (EC && EC != lmdb::DBError::Keyexist)
        return mapErrorCodeCommon(EC);
    } while (EC == lmdb::DBError::Keyexist);
    EC = DB->SymbolIDToISymbolDbi.put(Txn, ID.raw(), OidSlice);
    if (EC)
      return mapErrorCodeCommon(EC);
    for (const auto &V : ISymbol.Tokens) {
      EC = DB->HashedTokenToISymbolDbi.put(Txn, makeSlice(V), OidSlice);
      if (EC)
        return mapErrorCodeCommon(EC);
    }
  } else {
    OidSlice = makeSlice(*ExistingOid);
    Slice ISymbolSlice;
    EC = DB->IndexedSymbolsDbi.get(Txn, OidSlice, ISymbolSlice);
    if (EC)
      return mapErrorCodeCommon(EC);
    const auto &ExistingISymbol = ISymbolSlice.toTypeRef<cl::IndexedSymbol>();
    for (const auto &V : ExistingISymbol.Tokens) {
      EC = DB->HashedTokenToISymbolDbi.del(Txn, makeSlice(V), OidSlice);
      if (EC) {
        if (EC != lmdb::DBError::Notfound)
          return mapErrorCodeCommon(EC);
        return DSError::DBInconsistent;
      }
    }
    ISymbolSlice = SerializedISymbol;
    EC = DB->IndexedSymbolsDbi.put(Txn, OidSlice, ISymbolSlice);
    if (EC)
      return mapErrorCodeCommon(EC);
    for (const auto &V : ISymbol.Tokens) {
      EC = DB->HashedTokenToISymbolDbi.put(Txn, makeSlice(V), OidSlice);
      if (EC)
        return mapErrorCodeCommon(EC);
    }
  }
  return {};
}

std::error_code TransactionImpl::updateIndexedSymbols() {
  auto Stat = DB->UpdatedSymbolsDbi.stat(Txn);
  if (!Stat)
    return mapErrorCodeCommon(Stat.getError());
  if (Stat->ms_entries != 0) {
    auto CurOrErr = lmdb::Cursor::open(Txn, DB->UpdatedSymbolsDbi);
    if (!CurOrErr)
      return mapErrorCodeCommon(CurOrErr.getError());
    auto &Cursor = *CurOrErr;
    Slice Key;
    std::error_code EC;
    while (!(EC = Cursor.get(Key, MDB_NEXT))) {
      SymbolID SymID = Marshaller.toNative(Key.toTypeRef<cl::SymbolID>());
      EC = updateSymbol(SymID);
      if (EC)
        return EC;
    }
    if (EC != lmdb::DBError::Notfound)
      return mapErrorCodeCommon(EC);
    EC = DB->UpdatedSymbolsDbi.drop(Txn);
    if (EC)
      return mapErrorCodeCommon(EC);
  }
  return {};
}

namespace {
struct DbiNameAndFlags {
  std::string_view Name;
  unsigned int Flags;
  lmdb::DBI DocumentStoreImpl::*DbiMemberPtr;
} DbisTable[] = {
    {"IndexDataFileDbi", MDB_INTEGERKEY, &DocumentStoreImpl::IndexDataFilesDbi},
    {"IndexedSymbolsDbi", MDB_INTEGERKEY,
     &DocumentStoreImpl::IndexedSymbolsDbi},
    {"HashedURIToIDFileDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::HashedURIToIDFileDbi},
    {"HashedPathToIDFileDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::HashedPathToIDFileDbi},
    {"SymbolIDToIDFileDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::SymbolIDToIDFileDbi},
    {"RefSymbolIDToIDFileDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::RefSymbolIDToIDFileDbi},
    {"RelationKeyToIDFileDbi", MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP,
     &DocumentStoreImpl::RelationKeyToIDFileDbi},
    {"RevRefSymbolIDToIDFileDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::RevRefSymbolIDToIDFileDbi},
    {"SymbolIDToISymbolDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::SymbolIDToISymbolDbi},
    {"HashedTokenToISymbolDbi",
     MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERKEY | MDB_INTEGERDUP,
     &DocumentStoreImpl::HashedTokenToISymbolDbi},
    {"UpdatedSymbolsDbi", MDB_INTEGERKEY,
     &DocumentStoreImpl::UpdatedSymbolsDbi},
};
} // namespace

DocumentStoreImpl::DocumentStoreImpl(llvm::StringRef Path) {
  auto EnvOrErr = lmdb::Environment::create();
  if (!EnvOrErr)
    return;
  Env = std::move(*EnvOrErr);

  // Maximum 32 dbs for now.
  Env.setMaxDBs(32);

  unsigned int OpenFlags = MDB_NOTLS | MDB_NOMETASYNC | MDB_WRITEMAP;
  Env.open(Path.data(), OpenFlags);

  auto TxnOrErr = lmdb::Txn::begin(Env);
  if (!TxnOrErr)
    return;
  auto Txn = std::move(*TxnOrErr);

  for (const auto &N : DbisTable) {
    auto DbiOrErr = lmdb::DBI::open(Txn, N.Name, N.Flags | MDB_CREATE);
    if (!DbiOrErr)
      return;
    this->*N.DbiMemberPtr = *DbiOrErr;
  }

  Txn.commit();
}

DocumentStoreImpl::~DocumentStoreImpl() {
  ::MDB_envinfo EnvInfo = Env.getEnvInfo();
  log("~LMDBInstance(): mapsize = {0}, last_pgno = {1}, last_txnid = {2}",
      EnvInfo.me_mapsize, EnvInfo.me_last_pgno, EnvInfo.me_last_txnid);
}

std::shared_ptr<DocumentStoreImpl>
DocumentStoreImpl::createInstance(llvm::StringRef Path) {
  return std::shared_ptr<DocumentStoreImpl>(new DocumentStoreImpl(Path));
}

void DocumentStoreImpl::raiseFatalError(std::error_code EC) {
  llvm::report_fatal_error(
      llvm::Twine("Database fatal error! Error message: " + EC.message()));
}

llvm::ErrorOr<std::unique_ptr<Transaction>>
DocumentStoreImpl::beginTransaction(bool RO, llvm::StringRef Tag) {
  auto Txn = TransactionImpl::begin(*this, RO, Tag);
  if (!Txn)
    return Txn.getError();
  return std::move(*Txn);
}

std::error_code DocumentStoreImpl::growDatabaseMapSize(size_t OldMapSizeHint) {
  constexpr mdb_size_t SizeCap =
      mdb_size_t(2) * 1024 * 1024 * 1024 * 1024; // 2TB for now.
  EnvResizeLock EnvResizeLk =
      std::unique_lock<std::shared_mutex>(EnvResizeRWMu);
  size_t EnvMapSize = Env.getEnvInfo().me_mapsize;
  if (OldMapSizeHint != 0 && OldMapSizeHint < EnvMapSize) {
    // Other threads already updated the database size, so avoid repeating it.
    return {};
  }
  size_t TargetEnvMapSize = std::min(EnvMapSize << 1, SizeCap);
  if (auto EC = Env.setMapSize(TargetEnvMapSize))
    return mapErrorCodeCommon(EC);
  return {};
}

std::unique_ptr<IndexFileInfo>
DocumentStoreImpl::loadShardInfo(Transaction &TransIn,
                                 llvm::StringRef ShardPath) {
  TransactionImpl &Txn = static_cast<TransactionImpl &>(TransIn);
  return Txn.loadShardInfo(ShardPath);
}

std::error_code DocumentStoreImpl::updateShard(Transaction &TransIn,
                                               llvm::StringRef ShardPath,
                                               const IndexFileOut *Shard,
                                               int64_t ModifiedTime) {
  TransactionImpl &Txn = static_cast<TransactionImpl &>(TransIn);
  return Txn.updateShard(ShardPath, Shard, ModifiedTime);
}

std::shared_ptr<DocumentStore>
createSharedDatabase(llvm::StringRef DatabasePath) {
  if (!llvm::sys::fs::exists(DatabasePath)) {
    if (auto EC = llvm::sys::fs::create_directories(DatabasePath)) {
      elog("Failed to create LMDB database directory: {0}", DatabasePath);
      return nullptr;
    }
  }
  return DocumentStoreImpl::createInstance(
      static_cast<std::string>(DatabasePath));
}

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
