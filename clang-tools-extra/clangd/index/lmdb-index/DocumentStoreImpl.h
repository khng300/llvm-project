//===--- DocumentStoreImpl.h - Symbol database ------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_DOCUMENTSTOREIMPL_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_DOCUMENTSTOREIMPL_H

#include "DocumentStore.h"
#include "IndexDB.h"
#include "Serialization.h"
#include "Token.h"

#include <shared_mutex>

namespace clang {
namespace clangd {
namespace lmdb_index {

namespace key_id {

enum IndexDataFile : IndexID {
  IDXDATAFILE_SHARDPATH,
  IDXDATAFILE_URI,
};

enum IndexedSymbol : IndexID {
  INDEXEDSYMBOL_ID,
  INDEXEDSYMBOL_TOKEN,
};

} // namespace key_id

template <>
struct IndexCollectorTrait<cl::IndexDataFile>
    : IndexCollector<
          cl::IndexDataFile,
          IndexCollectorArg<
              key_id::IDXDATAFILE_SHARDPATH,
              key_extractors::Member<&cl::IndexDataFile::ShardPathHash>>,
          IndexCollectorArg<
              key_id::IDXDATAFILE_URI,
              key_extractors::Member<&cl::IndexDataFile::FileURIHash>>> {};

struct IndexedSymbolDatabase : DatabaseBase {
  IndexedSymbolDatabase(std::string_view NamePrefix)
      : DatabaseBase(NamePrefix, {0, MDB_INTEGERKEY | MDB_DUPFIXED}) {}

  std::error_code open(lmdb::Txn &Tx) override {
    return DatabaseBase::open(Tx);
  }

  virtual bool
  index(Slice ObjBufferRef,
        llvm::function_ref<bool(IndexID, Slice)> Callback) override {
    const auto &Obj = ObjBufferRef.toTypeRef<cl::IndexedSymbol>();
    bool Ret = Callback(key_id::INDEXEDSYMBOL_ID, makeSlice(Obj.Symbol.ID));
    if (!Ret)
      return false;
    for (const auto &V : Obj.Tokens) {
      Ret = Callback(key_id::INDEXEDSYMBOL_TOKEN, makeSlice(V));
      if (!Ret)
        return false;
    }
    return true;
  }

  llvm::ErrorOr<OID> allocOid(lmdb::Txn &Tx, const cl::IndexedSymbol &Obj) {
    return DatabaseBase::allocOid(Tx, serializeObject(Obj));
  }

  std::error_code removeOid(lmdb::Txn &Tx, OID Oid) {
    return DatabaseBase::removeOid(Tx, Oid);
  }

  std::error_code writeOid(lmdb::Txn &Tx, OID Oid,
                           const cl::IndexedSymbol &Obj) {
    return DatabaseBase::writeOid(Tx, Oid, serializeObject(Obj));
  }

  llvm::ErrorOr<IndexOIDIterator> findOid(lmdb::Txn &Tx, IndexID IndexId,
                                          Slice Key) {
    auto Result = DatabaseBase::findOid(Tx, IndexId, Key);
    if (!Result)
      return Result.getError();
    return IndexOIDCursor(std::move(*Result));
  }

  std::error_code
  findAllOfOidsByKey(lmdb::Txn &Tx, IndexID IndexId, Slice Key,
                     llvm::function_ref<bool(OID, const Type &)> Callback) {
    return DatabaseBase::findAllOfOidsByKey(
        Tx, IndexId, Key, [&](OID Oid, Slice ObjBufferRef) {
          return Callback(Oid, ObjBufferRef.toTypeRef<Type>());
        });
  }

  std::error_code removeAllOfOidsByKey(
      lmdb::Txn &Tx, IndexID IndexId, Slice Key,
      llvm::function_ref<void(OID, const Type &)> PreRemovalCallback =
          nullptr) {
    std::error_code EC, EC2;
    EC = findAllOfOidsByKey(Tx, IndexId, Key, [&](OID Oid, const Type &Obj) {
      if (PreRemovalCallback)
        PreRemovalCallback(Oid, Obj);
      return !(EC2 = removeOid(Tx, Oid));
    });
    return EC ? EC : EC2;
  }

  llvm::ErrorOr<OIDCursor> getOidCursor(lmdb::Txn &Txn) {
    auto Result = DatabaseBase::getOidCursor(Txn);
    if (!Result)
      return Result.getError();
    return OIDCursor(std::move(*Result));
  }

  llvm::ErrorOr<const cl::IndexedSymbol *> getOidPointer(lmdb::Txn &Tx,
                                                         OID Oid) {
    auto Res = DatabaseBase::getOid(Tx, Oid);
    if (!Res)
      return Res.getError();
    return &Res->toTypeRef<cl::IndexedSymbol>();
  }

  llvm::ErrorOr<cl::IndexedSymbol> readOid(lmdb::Txn &Tx, OID Oid) {
    auto ObjOrErr = getOidPointer(Tx, Oid);
    if (!ObjOrErr)
      return ObjOrErr.getError();
    return *ObjOrErr.get();
  }
};

class DocumentStoreImpl : public DocumentStore {
  friend class LMDBIndexImpl;
  friend class DocumentStoreQuery;
  friend std::unique_ptr<SymbolIndex>
  createSymbolIndex(std::shared_ptr<DocumentStore> DB);

  DocumentStoreImpl(llvm::StringRef ShardPath);

public:
  class TransactionImpl : public Transaction {
    typedef std::variant<std::shared_lock<std::shared_mutex>,
                         std::unique_lock<std::shared_mutex>>
        TxnLocker;

  public:
    class MarshallingHelper {
    public:
      MarshallingHelper() = delete;
      MarshallingHelper(DocumentStoreImpl::TransactionImpl *Txn) : Txn(Txn) {}

      cl::SymbolID fromNative(clangd::SymbolID);
      cl::FileDigest fromNative(clangd::FileDigest);
      cl::SymbolLocation::Position
      fromNative(const clangd::SymbolLocation::Position &);
      llvm::ErrorOr<cl::SymbolLocation>
      fromNative(const clangd::SymbolLocation &);
      cl::Symbol::IncludeHeaderWithReferences
      fromNative(const clangd::Symbol::IncludeHeaderWithReferences &);
      llvm::ErrorOr<cl::Symbol> fromNative(const clangd::Symbol &);
      llvm::ErrorOr<cl::Ref> fromNative(const clangd::Ref &);
      cl::Relation fromNative(const clangd::Relation &);
      llvm::ErrorOr<cl::RevRef> fromNative(const clangd::ContainedRefsResult &);
      cl::IncludeGraphNode fromNative(const clangd::IncludeGraphNode &);
      cl::IncludeGraph fromNative(const clangd::IncludeGraph &);
      cl::CompileCommand fromNative(const tooling::CompileCommand &);

      clangd::SymbolID toNative(cl::SymbolID);
      clangd::FileDigest toNative(cl::FileDigest);
      clangd::SymbolLocation::Position
      toNative(const cl::SymbolLocation::Position &);
      llvm::ErrorOr<clangd::SymbolLocation>
      toNative(const cl::SymbolLocation &);
      clangd::Symbol::IncludeHeaderWithReferences
      toNative(const cl::Symbol::IncludeHeaderWithReferences &);
      llvm::ErrorOr<clangd::Symbol> toNative(const cl::Symbol &);
      llvm::ErrorOr<clangd::Ref> toNative(const cl::Ref &);
      clangd::Relation toNative(const cl::Relation &);
      llvm::ErrorOr<clangd::ContainedRefsResult> toNative(const cl::RevRef &);
      clangd::IncludeGraphNode toNative(const cl::IncludeGraphNode &);
      clangd::IncludeGraph toNative(const cl::IncludeGraph &);
      tooling::CompileCommand toNative(const cl::CompileCommand &);

    private:
      DocumentStoreImpl::TransactionImpl *Txn;
    };

    virtual ~TransactionImpl() = default;

    static llvm::ErrorOr<std::unique_ptr<TransactionImpl>>
    begin(DocumentStoreImpl &DB, bool RO, llvm::StringRef Tag);

    lmdb::Txn &getTxn() { return Txn; }
    bool isReadOnly() const { return Txn.flags() & MDB_RDONLY; }

    std::error_code commit() && override;
    void abort() && override;

    [[noreturn]] void raiseFatalError(std::error_code EC);

    MarshallingHelper &getMarshaller() { return Marshaller; }

    llvm::ErrorOr<OID> createEmptyIndexDataFile(llvm::StringRef ShardPath,
                                                llvm::StringRef FileURI);
    llvm::ErrorOr<cl::IndexDataFile> toIndexDataFile(llvm::StringRef,
                                                     const IndexFileOut &,
                                                     int64_t ModifiedTime);
    llvm::ErrorOr<OID> createIndexDataFile(const cl::IndexDataFile &ShardInfo);
    llvm::ErrorOr<const cl::IndexDataFile *> getIndexDataFile(OID Oid);
    llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
    getIndexDataFileByShardPath(llvm::StringRef ShardPath);
    llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
    getIndexDataFileByURI(llvm::StringRef FileURI);
    std::error_code removeIndexFileInfo(OID Oid);
    std::error_code removeIndexDataFile(OID Oid);
    std::error_code writeIndexDataFile(OID Oid,
                                       const cl::IndexDataFile &IDFile);

    std::unique_ptr<IndexFileInfo> loadShardInfo(llvm::StringRef ShardPath);
    llvm::ErrorOr<OID> ensureShardExistence(llvm::StringRef FileURI);

    llvm::ErrorOr<cl::Symbol> lookupShardsSymbol(SymbolID ID);
    llvm::ErrorOr<cl::IndexedSymbol> findSymbol(SymbolID ID);

    llvm::ErrorOr<lmdb::Cursor> getKeyToFileDbiCursor(lmdb::DBI &Dbi,
                                                      Slice Key);

    std::error_code insertShardIndexes(OID ShardOid,
                                       const cl::IndexDataFile &IDFile);
    std::error_code removeShardIndexes(OID ShardOid,
                                       const cl::IndexDataFile &IDFile);

    std::error_code updateShard(llvm::StringRef ShardPath,
                                const IndexFileOut *Shard,
                                int64_t ModifiedTime);

    std::error_code updateIndexedSymbols() override;

  private:
    TransactionImpl(DocumentStoreImpl &DB, TxnLocker &&Lk, lmdb::Txn &&Txn,
                    llvm::StringRef Tag)
        : Tag(Tag), DB(DB.getInstance()), TxnLk(std::move(Lk)),
          Txn(std::move(Txn)), Marshaller(this) {}

    std::error_code updateSymbol(SymbolID ID);

    std::string Tag;
    std::shared_ptr<DocumentStoreImpl> DB;
    TxnLocker TxnLk;
    lmdb::Txn Txn;
    MarshallingHelper Marshaller;

    std::chrono::system_clock::duration IndexDuration;
    std::chrono::system_clock::duration UpdatedSymbolsDuration;
    std::chrono::system_clock::duration DatafileDuration;
  };

  ~DocumentStoreImpl() override;

  std::shared_ptr<DocumentStoreImpl> getInstance() {
    return std::static_pointer_cast<DocumentStoreImpl>(shared_from_this());
  }

  static std::shared_ptr<DocumentStoreImpl>
  createInstance(llvm::StringRef Path);

  [[noreturn]] void raiseFatalError(std::error_code EC);

  llvm::ErrorOr<std::unique_ptr<Transaction>>
  beginTransaction(bool RO, llvm::StringRef Tag = {}) override;

  size_t databaseMapSize() override;
  std::error_code growDatabaseMapSize() override;
  void refreshEnvMapSize();

  std::unique_ptr<IndexFileInfo>
  loadShardInfo(Transaction &TransIn, llvm::StringRef ShardPath) override;
  std::error_code updateShard(Transaction &TransIn, llvm::StringRef ShardPath,
                              const IndexFileOut *Shard,
                              int64_t ModifiedTime) override;

  Database<cl::IndexDataFile> IndexDataFilesDB{"DB_INDEX_DATA_FILES"};
  IndexedSymbolDatabase IndexedSymbolsDB{"DB_INDEXEDSYMBOLS"};

  std::shared_mutex TransactionMu;
  lmdb::Environment Env;
  std::atomic<size_t> EnvMapSize;

  lmdb::DBI SymbolIDToIDFileDbi;
  lmdb::DBI RefSymbolIDToIDFileDbi;
  lmdb::DBI RelationKeyToIDFileDbi;
  lmdb::DBI RevRefSymbolIDToIDFileDbi;

  lmdb::DBI UpdatedSymbolsDbi;
};

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
