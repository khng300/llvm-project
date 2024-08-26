//===--- IndexDB.h - Symbol database ----------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_INDEXDB_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_INDEXDB_H

#include "IndexDBError.h"
#include "LMDBWrapper.h"
#include "TypeTraits.h"
#include "llvm/Support/xxhash.h"

#include <system_error>
#include <tuple>
#include <unordered_set>

namespace clang {
namespace clangd {
namespace db_index {

typedef uint64_t OID;
typedef uint32_t IndexID;
typedef std::pair<IndexID, std::string> IndexPair;

constexpr OID EmptyOid = (OID)-1;

struct IndexHash {
  std::size_t operator()(const IndexPair &V) const noexcept {
    return llvm::hash_combine(V.first, V.second);
  }
};

//
// Convert DBError to the equivalent IndexError
//
// If there exists no conversion rule for a given DBError, just return that
// as-is.
//
std::error_code mapDBErrorToIndexDBError(std::error_code EC);

namespace key_extractors {

template <auto MemPtr> struct Member {
  constexpr decltype(auto) operator()(
      const typename MemberPointerTypeHelper<decltype(MemPtr)>::ClassType &Obj)
      const {
    return (Obj.*MemPtr);
  }
};

template <typename ObjectType, typename... Args> struct Composite {
  constexpr decltype(auto) operator()(const ObjectType &Obj) const {
    return std::make_tuple(std::invoke(Args(), Obj)...);
  }
};

template <typename Arg, typename... Args> struct Chained {
  constexpr decltype(auto) operator()(
      typename FuncSignatureHelper<decltype(&Arg::operator())>::ArgumentType A)
      const {
    decltype(auto) Result = std::invoke(Arg(), A);
    if constexpr (sizeof...(Args) == 0)
      return Result;
    else
      return Chained<Args...>()(Result);
  }
};

template <typename InType> struct Hashed {
  uint64_t operator()(const InType &Doc) const {
    return llvm::xxh3_64bits(llvm::StringRef(&Doc[0], Doc.size()));
  }
};

} // namespace key_extractors

template <IndexID Id, typename KeyExtractorCallable> struct IndexCollectorArg {
  static constexpr IndexID IndexId = Id;
  static constexpr KeyExtractorCallable Fn;
};

template <typename ObjectType, typename... Args> struct IndexCollector {
  using Arguments = std::tuple<Args...>;
  static constexpr std::size_t IndexCount = std::tuple_size_v<Arguments>;

  template <
      typename CollectCallbackType,
      typename = std::enable_if_t<std::is_same_v<
          std::invoke_result_t<CollectCallbackType, IndexID, llvm::StringRef>,
          bool>>>
  void operator()(const ObjectType &Obj, CollectCallbackType &&Callback) {
    trigger(std::make_index_sequence<IndexCount>(), Obj,
            std::forward<CollectCallbackType>(Callback));
  }

private:
  template <typename CollectCallbackType, std::size_t... I>
  constexpr bool trigger(std::index_sequence<I...>, const ObjectType &Obj,
                         CollectCallbackType &&Callback) {
    return (... &&
            invokeCallback(
                std::forward<CollectCallbackType>(Callback),
                std::tuple_element_t<I, Arguments>::IndexId,
                std::invoke(std::tuple_element_t<I, Arguments>::Fn, Obj)));
  }

  template <typename T, typename CollectCallbackType>
  constexpr static bool invokeCallback(CollectCallbackType &&Callback,
                                       IndexID IndexId, T &&V) {
    if constexpr (IsStringV<T>) {
      return Callback(IndexId, std::forward<T>(V));
    } else {
      return Callback(IndexId,
                      llvm::StringRef(reinterpret_cast<const char *>(&V),
                                      sizeof(std::remove_reference_t<T>)));
    }
  }
};

template <typename RetType>
const RetType &stringRefToTypeRef(llvm::StringRef V) {
  return *reinterpret_cast<const RetType *>(V.data());
}

template <typename Type> llvm::StringRef typeToStringRef(const Type &O) {
  return llvm::StringRef(
      reinterpret_cast<const char *>(&O),
      sizeof(std::remove_const_t<std::remove_reference_t<Type>>));
}

template <typename ObjectType, typename = void> struct Serialize {
  std::string operator()(ObjectType &&Obj) = delete;
};

template <typename ObjectType> std::string serializeObject(ObjectType &&Obj) {
  return Serialize<std::remove_const_t<std::remove_reference_t<ObjectType>>>()(
      Obj);
}

struct DatabaseIndexCursor {
  DatabaseIndexCursor() = delete;
  DatabaseIndexCursor(const DatabaseIndexCursor &It) = delete;
  DatabaseIndexCursor(DatabaseIndexCursor &&It) { moveAssign(std::move(It)); }
  DatabaseIndexCursor(lmdb::Cursor &&Cur) : Cursor(std::move(Cur)) {
    auto Err = getSetCursor(lmdb::DBOperation::GetCurrent);
    (void)Err;
    assert(!Err);
  }
  ~DatabaseIndexCursor() = default;

  DatabaseIndexCursor &operator=(const DatabaseIndexCursor &It) = delete;
  DatabaseIndexCursor &operator=(DatabaseIndexCursor &&It) noexcept {
    moveAssign(std::move(It));
    return *this;
  }

  OID getOID() const { return SavedOid; }

  llvm::ErrorOr<OID> gotoOID(OID Oid) {
    llvm::StringRef Key, Data;
    auto OidOrErr = getSetCursor(Key, Data, lmdb::DBOperation::GetCurrent);
    if (OidOrErr)
      return OidOrErr;
    Data = typeToStringRef(Oid);
    return getSetCursor(Key, Data, lmdb::DBOperation::GetBothRange);
  }

  llvm::ErrorOr<OID> prev() { return getSetCursor(lmdb::DBOperation::PrevDup); }
  llvm::ErrorOr<OID> next() { return getSetCursor(lmdb::DBOperation::NextDup); }

  OID count() { return *Cursor.count(); }

private:
  void moveAssign(DatabaseIndexCursor &&It) noexcept {
    if (&It == this)
      return;
    DatabaseIndexCursor::~DatabaseIndexCursor();
    std::swap(It.Cursor, Cursor);
    std::swap(It.SavedOid, SavedOid);
  }

  llvm::ErrorOr<OID> getSetCursor(llvm::StringRef &Key, llvm::StringRef &Data,
                                  lmdb::DBOperation Operation) {
    auto EC = Cursor.get(Key, Data, Operation);
    if (EC) {
      if (EC == lmdb::DBError::Notfound)
        return EmptyOid;
      return mapDBErrorToIndexDBError(EC);
    }
    saveOid(Data);
    return SavedOid;
  }

  llvm::ErrorOr<OID> getSetCursor(lmdb::DBOperation Operation) {
    llvm::StringRef Key, Data;
    return getSetCursor(Key, Data, Operation);
  }

  void saveOid(llvm::StringRef Data) {
    SavedOid = llvm::bit_cast<OID>(*reinterpret_cast<const OID *>(Data.data()));
  }

  lmdb::Cursor Cursor;
  OID SavedOid{EmptyOid};
};

struct DatabaseMethods {
  virtual ~DatabaseMethods() = default;

  virtual std::size_t databaseCount() = 0;
  virtual std::error_code open(lmdb::Txn &Tx) = 0;
};

template <typename ObjectType, typename IndexCollector>
struct Database : DatabaseMethods {
  using Type = ObjectType;
  using IndexCollectorType = IndexCollector;
  typedef std::unordered_set<IndexPair, IndexHash> IndexSet;

  std::size_t databaseCount() override {
    return 2 + IndexCollectorType::IndexCount;
  }

  Database(std::string_view NamePrefix) : NamePrefix(NamePrefix) {}
  std::error_code open(lmdb::Txn &Tx) override {
    std::string OidName = NamePrefix + "_OID";
    std::string FreedOidName = NamePrefix + "_FREED_OID";
    std::string OidKeysName = NamePrefix + "_OID_KEYS";
    auto DBIOrErr =
        lmdb::DBI::open(Tx, OidName.c_str(), MDB_INTEGERKEY | MDB_CREATE);
    if (!DBIOrErr)
      return mapDBErrorToIndexDBError(DBIOrErr.getError());
    Objects = std::move(*DBIOrErr);
    DBIOrErr =
        lmdb::DBI::open(Tx, FreedOidName.c_str(), MDB_INTEGERKEY | MDB_CREATE);
    if (!DBIOrErr)
      return mapDBErrorToIndexDBError(DBIOrErr.getError());
    FreedOids = std::move(*DBIOrErr);
    for (uint32_t J = 0; J < IndexCollector::IndexCount; J++) {
      std::string Name = NamePrefix + "_KEYS_OID_" + std::to_string(J);
      DBIOrErr = lmdb::DBI::open(Tx, Name.c_str(),
                                 MDB_DUPSORT | MDB_INTEGERDUP | MDB_CREATE);
      if (!DBIOrErr)
        return mapDBErrorToIndexDBError(DBIOrErr.getError());
      IndexOids[J] = std::move(*DBIOrErr);
    }
    return std::error_code();
  }

  llvm::ErrorOr<OID> numberOfEntries(lmdb::Txn &Tx) {
    auto Stat = Objects.stat(Tx);
    if (!Stat)
      return IndexDBError::InternalError;
    return Stat->ms_entries;
  }

  llvm::ErrorOr<OID> allocOid(lmdb::Txn &Tx, const ObjectType &Obj) {
    auto CurOrErr = lmdb::Cursor::open(Tx, FreedOids);
    if (!CurOrErr)
      return mapDBErrorToIndexDBError(CurOrErr.getError());
    auto &Cur = *CurOrErr;
    llvm::ErrorOr<OID> OidOrErr = [&]() -> llvm::ErrorOr<OID> {
      llvm::StringRef ValOid;
      auto EC = Cur.get(ValOid, lmdb::DBOperation::Next);
      if (EC && EC != lmdb::DBError::Notfound)
        return mapDBErrorToIndexDBError(EC);
      if (EC) {
        auto StatOrErr = Objects.stat(Tx);
        if (!StatOrErr)
          return mapDBErrorToIndexDBError(StatOrErr.getError());
        return StatOrErr->ms_entries;
      }
      OID Ret = stringRefToTypeRef<OID>(ValOid);
      EC = Cur.del();
      if (EC)
        return mapDBErrorToIndexDBError(EC);
      return Ret;
    }();
    if (!OidOrErr)
      return OidOrErr.getError();
    std::string Buf = serializeObject(Obj);
    llvm::StringRef Data = Buf;
    auto Oid = *OidOrErr;
    auto EC = Objects.put(Tx, typeToStringRef(Oid), Data);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    EC = insertIndexToOid(Tx, Oid, Obj);
    if (EC)
      return EC;
    return Oid;
  }

  std::error_code removeOid(lmdb::Txn &Tx, OID Oid) {
    llvm::StringRef Data;
    auto EC = Objects.get(Tx, typeToStringRef(Oid), Data);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    const ObjectType &Obj = stringRefToTypeRef<ObjectType>(Data);
    EC = removeIndexToOid(Tx, Oid, Obj);
    if (EC)
      return EC;
    return insertOidToFreetable(Tx, Oid);
  }

  std::error_code writeOid(lmdb::Txn &Tx, OID Oid, const ObjectType &Obj) {
    IndexSet OldIndexes, NewIndexes;
    auto EC = [&]() -> std::error_code {
      llvm::StringRef OldData;
      auto EC = Objects.get(Tx, typeToStringRef(Oid), OldData);
      if (EC)
        return mapDBErrorToIndexDBError(EC);
      if (OldData.empty())
        return std::error_code();
      const ObjectType &OldObj = stringRefToTypeRef<ObjectType>(OldData);
      Indexer(OldObj, [&](IndexID IndexId, llvm::StringRef D) {
        OldIndexes.emplace(IndexId, D.str());
        return true;
      });
      return std::error_code();
    }();
    if (EC)
      return EC;
    Indexer(Obj, [&](IndexID IndexId, llvm::StringRef D) {
      NewIndexes.emplace(IndexId, D.str());
      return true;
    });
    std::string Buf = serializeObject(Obj);
    llvm::StringRef Data = Buf;
    EC = Objects.put(Tx, typeToStringRef(Oid), Data, 0);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    return updateIndexesToOid(Tx, Oid, OldIndexes, NewIndexes);
  }

  llvm::ErrorOr<DatabaseIndexCursor> findOid(lmdb::Txn &Tx, IndexID IndexId,
                                             llvm::StringRef Key) {
    auto CurOrErr = lmdb::Cursor::open(Tx, IndexOids[IndexId]);
    if (!CurOrErr)
      return mapDBErrorToIndexDBError(CurOrErr.getError());
    auto &Cur = *CurOrErr;
    auto EC = Cur.get(Key, lmdb::DBOperation::Set);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    return std::move(Cur);
  }

  template <IndexID IndexId,
            typename KeyExtractor =
                decltype(std::tuple_element_t<
                         IndexId, typename IndexCollectorType::Arguments>::Fn),
            typename KeyType = std::remove_cv_t<std::remove_reference_t<
                std::invoke_result_t<KeyExtractor, ObjectType>>>,
            typename = std::enable_if_t<
                !std::is_convertible_v<KeyType, llvm::StringRef>>>
  llvm::ErrorOr<DatabaseIndexCursor> findOid(lmdb::Txn &Tx, KeyType &&Key) {
    return findOid(Tx, IndexId, typeToStringRef(Key));
  }

  llvm::ErrorOr<const ObjectType *> getOidPointer(lmdb::Txn &Tx, OID Oid) {
    llvm::StringRef ValData;
    auto EC = Objects.get(Tx, typeToStringRef(Oid), ValData);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    return &stringRefToTypeRef<ObjectType>(ValData);
  }

  llvm::ErrorOr<ObjectType> readOid(lmdb::Txn &Tx, OID Oid) {
    auto ObjOrErr = getOidPointer(Tx, Oid);
    if (!ObjOrErr)
      return ObjOrErr.getError();
    return *ObjOrErr.get();
  }

  std::error_code removeOidsByKey(
      lmdb::Txn &Tx, IndexID IndexId, llvm::StringRef Key,
      llvm::function_ref<void(const Type &)> EachObjectCallback = nullptr) {
    auto CurOrErr = lmdb::Cursor::open(Tx, IndexOids[IndexId]);
    if (!CurOrErr)
      return mapDBErrorToIndexDBError(CurOrErr.getError());
    auto &Cur = *CurOrErr;
    llvm::StringRef Val;
    std::error_code EC;
    for (bool Ended = !!(EC = Cur.get(Key, Val, lmdb::DBOperation::Set));
         !Ended;
         Ended = !!(EC = Cur.get(Key, Val, lmdb::DBOperation::NextDup))) {
      OID Oid = llvm::bit_cast<OID>(*reinterpret_cast<const OID *>(Val.data()));
      auto ObjOrErr = getOidPointer(Tx, Oid);
      if (!ObjOrErr)
        return ObjOrErr.getError();
      const auto &Obj = **ObjOrErr;
      if (EachObjectCallback)
        EachObjectCallback(Obj);
      llvm::SmallVector<std::pair<IndexID, llvm::StringRef>> OtherIndexes;
      Indexer(Obj, [&](IndexID IndexId2, llvm::StringRef D) {
        if (IndexId2 == IndexId)
          return true;
        OtherIndexes.emplace_back(std::pair(IndexId2, D));
        return true;
      });
      for (auto &[IndexId2, D] : OtherIndexes) {
        EC = IndexOids[IndexId2].del(Tx, D, typeToStringRef(Oid));
        if (EC == lmdb::DBError::Notfound)
          EC = std::error_code();
        if (EC)
          return mapDBErrorToIndexDBError(EC);
      }
      EC = insertOidToFreetable(Tx, Oid);
      if (EC)
        return mapDBErrorToIndexDBError(EC);
    }
    if (EC && EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
    EC = IndexOids[IndexId].del(Tx, Key);
    if (EC && EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
    return std::error_code();
  }

private:
  std::error_code insertOidToFreetable(lmdb::Txn &Tx, OID Oid) {
    auto EC = Objects.del(Tx, typeToStringRef(Oid));
    if (EC && EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
    llvm::StringRef Val;
    EC = FreedOids.put(Tx, typeToStringRef(Oid), Val);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
    return std::error_code();
  }

  std::error_code insertIndexToOid(lmdb::Txn &Tx, OID Oid,
                                   const ObjectType &Obj) {
    std::error_code EC;
    Indexer(Obj, [&](IndexID IndexId, llvm::StringRef D) {
      llvm::StringRef Val = typeToStringRef(Oid);
      EC = mapDBErrorToIndexDBError(IndexOids[IndexId].put(Tx, D, Val));
      return EC ? false : true;
    });
    return EC;
  }

  std::error_code removeIndexToOid(lmdb::Txn &Tx, OID Oid,
                                   const ObjectType &Obj) {
    std::error_code EC;
    Indexer(Obj, [&](IndexID IndexId, llvm::StringRef D) {
      EC = IndexOids[IndexId].del(Tx, D, typeToStringRef(Oid));
      if (EC == lmdb::DBError::Notfound)
        EC = std::error_code();
      EC = mapDBErrorToIndexDBError(EC);
      return EC ? false : true;
    });
    return EC;
  }

  std::error_code updateIndexesToOid(lmdb::Txn &Tx, OID Oid,
                                     IndexSet &OldIndexes,
                                     IndexSet &NewIndexes) {
    // The first pass remove unchanged indexes.
    for (auto It = OldIndexes.begin(), NextIt = OldIndexes.end();
         (It != OldIndexes.end() ? (NextIt = std::next(It), true) : false);
         It = NextIt) {
      if (auto NewIt = NewIndexes.find(*It); NewIt != NewIndexes.end()) {
        NewIndexes.erase(NewIt);
        OldIndexes.erase(It);
      }
    }

    // The second pass removes stale indexes.
    llvm::StringRef Val = typeToStringRef(Oid);
    for (const auto &I : OldIndexes) {
      const auto &[IndexId, D] = I;
      auto EC = mapDBErrorToIndexDBError(IndexOids[IndexId].del(Tx, D, Val));
      if (EC && EC != lmdb::DBError::Notfound)
        return mapDBErrorToIndexDBError(EC);
    }
    // The third pass inserts new indexes.
    for (const auto &I : NewIndexes) {
      const auto &[IndexId, D] = I;
      auto EC = mapDBErrorToIndexDBError(IndexOids[IndexId].put(Tx, D, Val));
      if (EC)
        return mapDBErrorToIndexDBError(EC);
    }
    return std::error_code();
  }

  std::string NamePrefix;
  lmdb::DBI Objects;
  lmdb::DBI FreedOids;
  IndexCollectorType Indexer;
  std::array<lmdb::DBI, IndexCollectorType::IndexCount> IndexOids;
};

} // namespace db_index
} // namespace clangd
} // namespace clang

#endif