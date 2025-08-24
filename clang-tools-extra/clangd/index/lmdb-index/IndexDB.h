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
namespace lmdb_index {

using Slice = lmdb::Slice;
using lmdb::makeSlice;

typedef uint32_t IndexID;
typedef uint64_t OID;

// NOLINTBEGIN(readability-identifier-naming)
constexpr OID OID_None = 0;
// NOLINTEND(readability-identifier-naming)

inline uint64_t generateObjectHash(Slice ObjBuffer) {
  return llvm::xxh3_64bits(llvm::StringRef(ObjBuffer));
}

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
    decltype(auto) Res = std::invoke(Arg(), A);
    if constexpr (sizeof...(Args) == 0)
      return Res;
    else
      return Chained<Args...>()(Res);
  }
};

} // namespace key_extractors

template <IndexID Id, typename KeyExtractorCallable> struct IndexCollectorArg {
  static constexpr IndexID IndexId = Id;
  static constexpr KeyExtractorCallable Fn;
};

template <typename ObjectType, typename... Args> struct IndexCollector {
  using IndexRows = std::tuple<Args...>;
  static constexpr std::size_t IndexCount = std::tuple_size_v<IndexRows>;

  template <
      typename CollectCallbackType,
      typename = std::enable_if_t<std::is_same_v<
          std::invoke_result_t<CollectCallbackType, IndexID, Slice>, bool>>>
  bool operator()(const ObjectType &Obj, CollectCallbackType &&Callback) {
    return trigger(std::make_index_sequence<IndexCount>(), Obj,
                   std::forward<CollectCallbackType>(Callback));
  }

private:
  template <typename CollectCallbackType, std::size_t... I>
  constexpr bool trigger(std::index_sequence<I...>, const ObjectType &Obj,
                         CollectCallbackType &&Callback) {
    return (... &&
            invokeCallback(
                std::forward<CollectCallbackType>(Callback),
                std::tuple_element_t<I, IndexRows>::IndexId,
                std::invoke(std::tuple_element_t<I, IndexRows>::Fn, Obj)));
  }

  template <typename T, typename CollectCallbackType>
  constexpr static bool invokeCallback(CollectCallbackType &&Callback,
                                       IndexID IndexId, T &&V) {
    if constexpr (HasDataAndSizeMethodsV<T>) {
      return Callback(IndexId, Slice(V.data(), V.size()));
    } else {
      return Callback(IndexId, Slice(&V, sizeof(T)));
    }
  }
};

template <typename ObjectType = void> struct IndexCollectorTrait {
  using IndexRows = std::tuple<>;
  static constexpr std::size_t IndexCount = std::tuple_size_v<IndexRows>;

  template <
      typename CollectCallbackType,
      typename = std::enable_if_t<std::is_same_v<
          std::invoke_result_t<CollectCallbackType, IndexID, Slice>, bool>>>
  bool operator()(const ObjectType &Obj, CollectCallbackType &&Callback) {
    return true;
  }
};

template <typename ObjectType, typename = void> struct Serialize {
  std::string operator()(ObjectType &&Obj) = delete;
};

template <typename ObjectType> std::string serializeObject(ObjectType &&Obj) {
  return Serialize<std::remove_cv_t<std::remove_reference_t<ObjectType>>>()(
      Obj);
}

template <bool IsDup> struct DatabaseCursor {
  typedef llvm::ErrorOr<std::optional<Slice>> Result;

  DatabaseCursor(const DatabaseCursor &It) = delete;
  DatabaseCursor(DatabaseCursor &&It) { moveAssign(std::move(It)); }
  DatabaseCursor(lmdb::Cursor &&Cur) : Cursor(std::move(Cur)) {}

  ~DatabaseCursor() = default;

  DatabaseCursor &operator=(const DatabaseCursor &It) = delete;
  DatabaseCursor &operator=(DatabaseCursor &&It) noexcept {
    moveAssign(std::move(It));
    return *this;
  }

  Result setTo(Slice Key) {
    if (IsDup) {
      Slice Key1, Key2;
      auto Res = getSetCursorImpl(Key1, Key2, MDB_GET_CURRENT);
      if (!Res)
        return Res.getError();
      if (!*Res)
        return std::nullopt;
      Key2 = Key;
      Res = getSetCursorImpl(Key1, Key2, MDB_GET_BOTH_RANGE);
      if (!Res)
        return Res.getError();
      if (!*Res)
        return std::nullopt;
      return Key2;
    }
    Slice Data;
    auto Res = getSetCursorImpl(Key, Data, MDB_SET_RANGE);
    if (!Res)
      return Res.getError();
    if (!*Res)
      return std::nullopt;
    return Key;
  }

  Result current() { return getSetCursor(MDB_GET_CURRENT); }

  Result first() { return getSetCursor(IsDup ? MDB_FIRST_DUP : MDB_FIRST); }

  Result last() { return getSetCursor(IsDup ? MDB_LAST_DUP : MDB_LAST); }

  Result prev() { return getSetCursor(IsDup ? MDB_PREV_DUP : MDB_PREV_NODUP); }

  Result next() { return getSetCursor(IsDup ? MDB_NEXT_DUP : MDB_NEXT_NODUP); }

  OID count() {
    if (!Cursor.valid())
      llvm::report_fatal_error("Use of iterator after it is invalidated.");
    return *Cursor.count();
  }

protected:
  void moveAssign(DatabaseCursor &&It) noexcept {
    if (&It == this)
      return;
    DatabaseCursor::~DatabaseCursor();
    std::swap(It.Cursor, Cursor);
  }

  llvm::ErrorOr<bool> getSetCursorImpl(Slice &Key, Slice &Data,
                                       ::MDB_cursor_op Operation) {
    if (!Cursor.valid())
      llvm::report_fatal_error("Use of iterator after it is invalidated.");
    auto EC = mapDBErrorToIndexDBError(Cursor.get(Key, Data, Operation));
    if (EC) {
      if (EC != IndexDBError::Notfound)
        return EC;
      return false;
    }
    return true;
  }

  Result getSetCursor(::MDB_cursor_op Operation) {
    Slice S1, S2;
    auto Res = getSetCursorImpl(S1, S2, Operation);
    if (!Res)
      return Res.getError();
    if (!*Res)
      return std::nullopt;
    return IsDup ? S2 : S1;
  }

  lmdb::Cursor Cursor;
};

struct IndexOIDCursor : private DatabaseCursor<true> {
  typedef llvm::ErrorOr<std::optional<OID>> Result;

  IndexOIDCursor(const IndexOIDCursor &It) = delete;
  IndexOIDCursor(IndexOIDCursor &&It) = default;
  IndexOIDCursor(lmdb::Cursor &&Cur) : DatabaseCursor(std::move(Cur)) {}

  ~IndexOIDCursor() = default;

  IndexOIDCursor &operator=(const IndexOIDCursor &It) = delete;
  IndexOIDCursor &operator=(IndexOIDCursor &&It) = default;

  Result gotoOid(OID Oid) {
    return invoker(&DatabaseCursor::setTo, makeSlice(Oid));
  }

  Result current() { return invoker(&DatabaseCursor::current); }

  Result first() { return invoker(&DatabaseCursor::first); }

  Result last() { return invoker(&DatabaseCursor::last); }

  Result prev() { return invoker(&DatabaseCursor::prev); }

  Result next() { return invoker(&DatabaseCursor::next); }

  OID count() { return DatabaseCursor::count(); }

private:
  template <typename FnRet, typename... FnParams, typename... InvokeArgs>
  Result invoker(FnRet (DatabaseCursor::*Fn)(FnParams...), InvokeArgs... Args) {
    auto Res = (static_cast<DatabaseCursor *>(this)->*Fn)(Args...);
    if (!Res)
      return Res.getError();
    if (!*Res)
      return std::nullopt;
    return Res->value().template toTypeRef<OID>();
  }
};

struct IndexCursor : private DatabaseCursor<false> {
  typedef llvm::ErrorOr<std::optional<IndexOIDCursor>> Result;

  IndexCursor(const IndexCursor &It) = delete;
  IndexCursor(IndexCursor &&It) = default;
  IndexCursor(lmdb::Cursor &&Cur) : DatabaseCursor(std::move(Cur)) {}

  ~IndexCursor() = default;

  IndexCursor &operator=(const IndexCursor &It) = delete;
  IndexCursor &operator=(IndexCursor &&It) = default;

  Result gotoKey(Slice Key) { return invoker(&DatabaseCursor::setTo, Key); }

  Result current() { return invoker(&DatabaseCursor::current); }

  Result first() { return invoker(&DatabaseCursor::first); }

  Result last() { return invoker(&DatabaseCursor::last); }

  Result prev() { return invoker(&DatabaseCursor::prev); }

  Result next() { return invoker(&DatabaseCursor::next); }

  uint64_t count() { return DatabaseCursor::count(); }

private:
  template <typename FnRet, typename... FnParams, typename... InvokeArgs>
  Result invoker(FnRet (DatabaseCursor::*Fn)(FnParams...), InvokeArgs... Args) {
    auto Res = (static_cast<DatabaseCursor *>(this)->*Fn)(Args...);
    if (!Res)
      return Res.getError();
    if (!*Res)
      return std::nullopt;
    auto SubCur = Cursor.dup();
    if (!SubCur)
      return SubCur.getError();
    return IndexOIDCursor(std::move(*SubCur));
  }
};

struct OIDCursor : private DatabaseCursor<false> {
  typedef llvm::ErrorOr<std::optional<OID>> Result;

  OIDCursor(const OIDCursor &It) = delete;
  OIDCursor(OIDCursor &&It) = default;
  OIDCursor(lmdb::Cursor &&Cur) : DatabaseCursor(std::move(Cur)) {}

  ~OIDCursor() = default;

  OIDCursor &operator=(const OIDCursor &It) = delete;
  OIDCursor &operator=(OIDCursor &&It) = default;

  Result gotoOid(OID Oid) {
    return invoker(&DatabaseCursor::setTo, makeSlice(Oid));
  }

  Result current() { return invoker(&DatabaseCursor::current); }

  Result first() { return invoker(&DatabaseCursor::first); }

  Result last() { return invoker(&DatabaseCursor::last); }

  Result prev() { return invoker(&DatabaseCursor::prev); }

  Result next() { return invoker(&DatabaseCursor::next); }

  OID count() { return DatabaseCursor::count(); }

private:
  template <typename FnRet, typename... FnParams, typename... InvokeArgs>
  Result invoker(FnRet (DatabaseCursor::*Fn)(FnParams...), InvokeArgs... Args) {
    auto Res = (static_cast<DatabaseCursor *>(this)->*Fn)(Args...);
    if (!Res)
      return Res.getError();
    if (!*Res)
      return std::nullopt;
    return Res->value().template toTypeRef<OID>();
  }
};

class IndexOIDIterator : IndexOIDCursor {
  std::error_code ErrorCode;

  template <typename FnRet, typename... FnParams, typename... InvokeArgs>
  std::optional<OID> invoker(FnRet (IndexOIDCursor::*Fn)(FnParams...),
                             InvokeArgs... Args) {
    auto Res = (static_cast<IndexOIDCursor *>(this)->*Fn)(Args...);
    ErrorCode = Res.getError();
    if (!Res || !*Res)
      return std::nullopt;
    return **Res;
  }

public:
  typedef std::optional<OID> Result;

  IndexOIDIterator(IndexOIDCursor &&Cur) : IndexOIDCursor(std::move(Cur)) {}

  Result gotoOid(OID Oid) { return invoker(&IndexOIDCursor::gotoOid, Oid); }

  Result peek() { return invoker(&IndexOIDCursor::current); }

  Result next() { return invoker(&IndexOIDCursor::next); }

  OID count() { return IndexOIDCursor::count(); }

  std::error_code getLastError() const { return ErrorCode; }
};

class OIDIterator : OIDCursor {
  std::error_code ErrorCode;

  template <typename FnRet, typename... FnParams, typename... InvokeArgs>
  std::optional<OID> invoker(FnRet (OIDCursor::*Fn)(FnParams...),
                             InvokeArgs... Args) {
    auto Res = (static_cast<OIDCursor *>(this)->*Fn)(Args...);
    ErrorCode = Res.getError();
    if (!Res || !*Res)
      return std::nullopt;
    return **Res;
  }

public:
  typedef std::optional<OID> Result;

  OIDIterator(OIDCursor &&Cur) : OIDCursor(std::move(Cur)) {}

  Result gotoOid(OID Oid) { return invoker(&OIDCursor::gotoOid, Oid); }

  Result peek() { return invoker(&OIDCursor::current); }

  Result next() { return invoker(&OIDCursor::next); }

  OID count() { return OIDCursor::count(); }

  std::error_code getLastError() const { return ErrorCode; }
};

class DatabaseBase {
protected:
  typedef std::pair<IndexID, std::vector<unsigned char>> IndexPair;
  struct IndexHash {
    std::size_t operator()(const IndexPair &V) const noexcept {
      return llvm::hash_combine(
          V.first, llvm::hash_combine_range(V.second.begin(), V.second.end()));
    }
  };
  typedef std::unordered_set<IndexPair, IndexHash> IndexSet;

public:
  virtual ~DatabaseBase() = default;

  virtual std::error_code open(lmdb::Txn &Txn);
  std::size_t databaseCount() { return 2 + IndexOids.size(); }

  virtual bool index(Slice ObjSlice,
                     llvm::function_ref<bool(IndexID, Slice)> Callback) {
    return true;
  }

  llvm::ErrorOr<OID> numberOfEntries(lmdb::Txn &Txn);

  llvm::ErrorOr<std::optional<OID>> lastOid(lmdb::Txn &Txn);

protected:
  DatabaseBase(std::string_view NamePrefix, unsigned int NumIndexColumns)
      : NamePrefix(NamePrefix), NumOfIndexColumns(NumIndexColumns) {
    IndexColumnSettings.resize(NumIndexColumns);
    IndexOids.resize(NumOfIndexColumns);
  }
  DatabaseBase(std::string_view NamePrefix,
               llvm::ArrayRef<unsigned int> IndexSettings)
      : NamePrefix(NamePrefix), NumOfIndexColumns(IndexSettings.size()),
        IndexColumnSettings(IndexSettings) {
    IndexOids.resize(NumOfIndexColumns);
  }

  llvm::ErrorOr<OID> allocOid(lmdb::Txn &Txn, Slice ObjSlice);
  std::error_code removeOid(lmdb::Txn &Txn, OID Oid);
  std::error_code writeOid(lmdb::Txn &Txn, OID Oid, Slice ObjSlice);
  llvm::ErrorOr<Slice> getOid(lmdb::Txn &Txn, OID Oid);
  llvm::ErrorOr<IndexOIDCursor> findOid(lmdb::Txn &Txn, IndexID IndexId,
                                        Slice Key);
  std::error_code
  findAllOfOidsByKey(lmdb::Txn &Txn, IndexID IndexId, Slice Key,
                     llvm::function_ref<bool(OID, Slice)> Callback);

  llvm::ErrorOr<IndexCursor> getIndexCursor(lmdb::Txn &Txn, IndexID IndexId);

  llvm::ErrorOr<OIDCursor> getOidCursor(lmdb::Txn &Txn);

private:
  std::error_code insertOidToFreetable(lmdb::Txn &Txn, OID Oid);

  std::error_code insertIndexToOid(lmdb::Txn &Txn, OID Oid, Slice ObjSlice);
  std::error_code removeIndexToOid(lmdb::Txn &Txn, OID Oid, Slice ObjSlice);
  std::error_code updateIndexesToOid(lmdb::Txn &Txn, OID Oid,
                                     IndexSet &OldIndexes,
                                     IndexSet &NewIndexes);

  std::string NamePrefix;
  unsigned int NumOfIndexColumns;
  std::vector<unsigned int> IndexColumnSettings;
  lmdb::DBI Objects;
  lmdb::DBI FreedOids;
  std::vector<lmdb::DBI> IndexOids;
};

template <typename ObjectType> struct Database : DatabaseBase {
  using Type = ObjectType;
  using IndexCollectorType = IndexCollectorTrait<ObjectType>;

  Database(std::string_view NamePrefix)
      : DatabaseBase(NamePrefix, IndexColumnSettings) {}

  std::error_code open(lmdb::Txn &Txn) override {
    return DatabaseBase::open(Txn);
  }

  virtual bool
  index(Slice ObjSlice,
        llvm::function_ref<bool(IndexID, Slice)> Callback) override {
    return Indexer(ObjSlice.toTypeRef<ObjectType>(),
                   [&](IndexID IndexId, Slice IndexData) {
                     return Callback(IndexId, IndexData);
                   });
  }

  llvm::ErrorOr<OID> allocOid(lmdb::Txn &Txn, const ObjectType &Obj) {
    return DatabaseBase::allocOid(Txn, serializeObject(Obj));
  }

  std::error_code removeOid(lmdb::Txn &Txn, OID Oid) {
    return DatabaseBase::removeOid(Txn, Oid);
  }

  std::error_code writeOid(lmdb::Txn &Txn, OID Oid, const ObjectType &Obj) {
    return DatabaseBase::writeOid(Txn, Oid, serializeObject(Obj));
  }

  llvm::ErrorOr<IndexOIDIterator> findOid(lmdb::Txn &Txn, IndexID IndexId,
                                          Slice Key) {
    auto Result = DatabaseBase::findOid(Txn, IndexId, Key);
    if (!Result)
      return Result.getError();
    return IndexOIDCursor(std::move(*Result));
  }

  std::error_code
  findAllOfOidsByKey(lmdb::Txn &Txn, IndexID IndexId, Slice Key,
                     llvm::function_ref<bool(OID, const Type &)> Callback) {
    return DatabaseBase::findAllOfOidsByKey(
        Txn, IndexId, Key, [&](OID Oid, Slice ObjSlice) {
          return Callback(Oid, ObjSlice.toTypeRef<Type>());
        });
  }

  llvm::ErrorOr<IndexCursor> getIndexCursor(lmdb::Txn &Txn, IndexID IndexId) {
    auto Result = DatabaseBase::getIndexCursor(Txn, IndexId);
    if (!Result)
      return Result.getError();
    return IndexCursor(std::move(*Result));
  }

  llvm::ErrorOr<OIDCursor> getOidCursor(lmdb::Txn &Txn) {
    auto Result = DatabaseBase::getOidCursor(Txn);
    if (!Result)
      return Result.getError();
    return OIDCursor(std::move(*Result));
  }

  llvm::ErrorOr<const ObjectType *> getOidPointer(lmdb::Txn &Txn, OID Oid) {
    auto Res = DatabaseBase::getOid(Txn, Oid);
    if (!Res)
      return Res.getError();
    return &Res->template toTypeRef<ObjectType>();
  }

  llvm::ErrorOr<ObjectType> readOid(lmdb::Txn &Txn, OID Oid) {
    auto ObjOrErr = getOidPointer(Txn, Oid);
    if (!ObjOrErr)
      return ObjOrErr.getError();
    return *ObjOrErr.get();
  }

private:
  static constexpr std::size_t IndexCount = IndexCollectorType::IndexCount;

  template <size_t... IndexId>
  static constexpr std::array<unsigned int, sizeof...(IndexId)>
  indexColumnSettingImpl(std::index_sequence<IndexId...>) {
    return {(std::is_integral_v<std::remove_reference_t<decltype(std::invoke(
                     std::tuple_element_t<
                         IndexId, typename IndexCollectorType::IndexRows>::Fn,
                     std::declval<ObjectType>()))>>
                 ? MDB_INTEGERKEY
                 : 0)...};
  }
  static constexpr auto indexColumnSetting() {
    return indexColumnSettingImpl(std::make_index_sequence<IndexCount>());
  }

  constexpr static auto IndexColumnSettings = indexColumnSetting();
  IndexCollectorType Indexer;
};

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
