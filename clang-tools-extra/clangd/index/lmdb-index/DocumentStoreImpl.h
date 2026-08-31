//===--- DocumentStoreImpl.h - Symbol database ------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_DOCUMENTSTOREIMPL_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_DOCUMENTSTOREIMPL_H

#include "DocumentStore.h"
#include "LMDBWrapper.h"
#include "Serialization.h"
#include "Token.h"

#include <shared_mutex>

namespace clang {
namespace clangd {
namespace lmdb_index {

using Slice = lmdb::Slice;
using lmdb::makeSlice;

inline uint64_t generateObjectHash(Slice ObjBuffer) {
  return llvm::xxh3_64bits(llvm::StringRef(ObjBuffer));
}

namespace internal {
//
// Translate common error code to the equivalent DSError
//
// If there exists no conversion rule for a given error code, just return that
// as-is.
//
std::error_code mapErrorCodeCommon(std::error_code EC);
} // namespace internal

class DocumentStoreImpl : public DocumentStore {
  friend class LMDBIndexImpl;
  friend class DocumentStoreQuery;
  friend class TransactionImpl;
  friend std::unique_ptr<SymbolIndex>
  createSymbolIndex(std::shared_ptr<DocumentStore> DB);

  DocumentStoreImpl(llvm::StringRef ShardPath);

  typedef std::variant<std::shared_lock<std::shared_mutex>,
                       std::unique_lock<std::shared_mutex>>
      EnvResizeLock;

public:
  ~DocumentStoreImpl() override;

  std::shared_ptr<DocumentStoreImpl> getInstance() {
    return std::static_pointer_cast<DocumentStoreImpl>(shared_from_this());
  }

  static std::shared_ptr<DocumentStoreImpl>
  createInstance(llvm::StringRef Path);

  [[noreturn]] void raiseFatalError(std::error_code EC);

  llvm::ErrorOr<std::unique_ptr<Transaction>>
  beginTransaction(bool RO, llvm::StringRef Tag = {}) override;

  std::error_code growDatabaseMapSize(size_t OldMapSizeHint) override;

  std::unique_ptr<IndexFileInfo>
  loadShardInfo(Transaction &TransIn, llvm::StringRef ShardPath) override;
  std::error_code updateShard(Transaction &TransIn, llvm::StringRef ShardPath,
                              const IndexFileOut *Shard,
                              int64_t ModifiedTime) override;

  std::shared_mutex EnvResizeRWMu;
  lmdb::Environment Env;

  lmdb::DBI IndexDataFilesDbi;
  lmdb::DBI IndexedSymbolsDbi;

  lmdb::DBI HashedURIToIDFileDbi;
  lmdb::DBI HashedPathToIDFileDbi;
  lmdb::DBI SymbolIDToIDFileDbi;
  lmdb::DBI RefSymbolIDToIDFileDbi;
  lmdb::DBI RelationKeyToIDFileDbi;
  lmdb::DBI RevRefSymbolIDToIDFileDbi;

  lmdb::DBI SymbolIDToISymbolDbi;
  lmdb::DBI HashedTokenToISymbolDbi;

  lmdb::DBI UpdatedSymbolsDbi;
};

class TransactionImpl : public Transaction {
  typedef DocumentStoreImpl::EnvResizeLock EnvResizeLock;

public:
  class MarshallingHelper {
  public:
    MarshallingHelper() = delete;
    MarshallingHelper(TransactionImpl *Txn) : Txn(Txn) {}

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
    llvm::ErrorOr<clangd::SymbolLocation> toNative(const cl::SymbolLocation &);
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
    TransactionImpl *Txn;
  };

  class KeyToIDFilesIterator : lmdb::DupIterator {
    using Base = lmdb::DupIterator;

  public:
    KeyToIDFilesIterator(TransactionImpl &Txn, lmdb::Cursor &&Cur)
        : Base(std::move(Cur)), Txn(&Txn) {}
    KeyToIDFilesIterator(TransactionImpl &Txn, lmdb::DupIterator &&It)
        : Base(std::move(It)), Txn(&Txn) {}

    Base &&getDupIterator() && { return std::move(static_cast<Base &>(*this)); }

    std::error_code errorCode() const { return ErrorCode; }

    std::optional<std::pair<OID, const cl::IndexDataFile *>> peek() {
      return opImpl(MDB_GET_CURRENT);
    }
    std::optional<std::pair<OID, const cl::IndexDataFile *>> prev() {
      return opImpl(MDB_PREV_DUP);
    }
    std::optional<std::pair<OID, const cl::IndexDataFile *>> next() {
      return opImpl(MDB_NEXT_DUP);
    }

  private:
    std::optional<std::pair<OID, const cl::IndexDataFile *>>
    opImpl(MDB_cursor_op Op);

    TransactionImpl *Txn = nullptr;
    std::error_code ErrorCode;
  };

  virtual ~TransactionImpl() = default;

  static llvm::ErrorOr<std::unique_ptr<TransactionImpl>>
  begin(DocumentStoreImpl &DB, bool RO, llvm::StringRef Tag);

  lmdb::Txn &getTxn() { return Txn; }
  bool isReadOnly() const { return Txn.flags() & MDB_RDONLY; }

  size_t databaseMapSize() override;

  std::error_code commit() && override;
  void abort() && override;

  [[noreturn]] void raiseFatalError(std::error_code EC);

  MarshallingHelper &getMarshaller() { return Marshaller; }

  llvm::ErrorOr<OID> createEmptyIndexDataFile(llvm::StringRef ShardPath,
                                              llvm::StringRef FileURI);
  llvm::ErrorOr<cl::IndexDataFile>
  toIndexDataFile(llvm::StringRef, const IndexFileOut &, int64_t ModifiedTime);
  llvm::ErrorOr<OID> createIndexDataFile(const cl::IndexDataFile &ShardInfo);
  llvm::ErrorOr<const cl::IndexDataFile *> getIndexDataFile(OID Oid);
  llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
  getIndexDataFileByShardPath(llvm::StringRef ShardPath);
  llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
  getIndexDataFileByURI(llvm::StringRef FileURI);
  std::error_code removeIndexDataFile(OID Oid);
  std::error_code writeIndexDataFile(OID Oid, const cl::IndexDataFile &IDFile);

  std::unique_ptr<IndexFileInfo> loadShardInfo(llvm::StringRef ShardPath);
  llvm::ErrorOr<OID> ensureShardExistence(llvm::StringRef FileURI);

  llvm::ErrorOr<cl::Symbol> lookupShardsSymbol(SymbolID ID);
  llvm::ErrorOr<cl::IndexedSymbol> findSymbol(SymbolID ID);

  llvm::ErrorOr<KeyToIDFilesIterator> getKeyToFileDbiIterator(lmdb::DBI &Dbi,
                                                              Slice Key);

  std::error_code insertShardIndexes(OID ShardOid,
                                     const cl::IndexDataFile &IDFile);
  std::error_code removeShardIndexes(OID ShardOid,
                                     const cl::IndexDataFile &IDFile);

  std::error_code updateShard(llvm::StringRef ShardPath,
                              const IndexFileOut *Shard, int64_t ModifiedTime);

  std::error_code updateIndexedSymbols() override;

private:
  TransactionImpl(DocumentStoreImpl &DB, EnvResizeLock &&EnvResizeLk,
                  lmdb::Txn &&Txn, llvm::StringRef Tag)
      : Tag(Tag), DB(DB.getInstance()), EnvResizeLk(std::move(EnvResizeLk)),
        Txn(std::move(Txn)), Marshaller(this) {}

  OID getNewOid();

  std::error_code updateSymbol(SymbolID ID);

  std::string Tag;
  std::shared_ptr<DocumentStoreImpl> DB;
  EnvResizeLock EnvResizeLk;
  lmdb::Txn Txn;
  MarshallingHelper Marshaller;
  uint64_t AllocationCounter;

  std::chrono::system_clock::duration IndexDuration;
  std::chrono::system_clock::duration UpdatedSymbolsDuration;
  std::chrono::system_clock::duration DatafileDuration;
};

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
