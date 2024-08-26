//===--- LMDBDocumentStore.h - Symbol database ------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_DOCUMENTSTORE_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_DOCUMENTSTORE_H

#include "IndexDB.h"
#include "Iterator.h"
#include "LMDBEngine.h"
#include "Serialization.h"
#include "Token.h"

#include <shared_mutex>

namespace clang {
namespace clangd {
namespace db_index {
namespace key_id {

enum Shard : IndexID {
  SHARD_SHARDPATH,
  SHARD_URI,
};
enum ShardSymbol : IndexID {
  SHARD_SYMBOL_PROVIDER,
  SHARD_SYMBOL_ID,
};
enum ShardRefs : IndexID {
  SHARD_REFS_PROVIDER,
  SHARD_REFS_ID,
};
enum ShardRelation : IndexID {
  SHARD_RELATION_PROVIDER,
  SHARD_RELATION_KEY,
};
enum IndexedSymbol : IndexID {
  INDEXEDSYMBOL_ID,
  INDEXEDSYMBOL_TOKEN,
};

} // namespace key_id

namespace cl {

template <typename DataType> struct ShardDataRow {
  OID Provider;
  DataType Data;
};

using ShardSymbolRow = ShardDataRow<Symbol>;
using ShardRefsRow =
    ShardDataRow<data::pair<cl::SymbolID, data::vector<cl::Ref>>>;
using ShardRelationRow = ShardDataRow<Relation>;
using ShardIncludeGraphRow = ShardDataRow<IncludeGraph>;

} // namespace cl

namespace indexer {

using namespace key_extractors;

using Shard = IndexCollector<
    cl::IndexFileInfo,
    IndexCollectorArg<key_id::SHARD_SHARDPATH,
                      Chained<Member<&cl::IndexFileInfo::ShardPath>,
                              Hashed<cl::data::cstring>>>,
    IndexCollectorArg<key_id::SHARD_URI,
                      Chained<Member<&cl::IndexFileInfo::FileUri>,
                              Hashed<cl::data::cstring>>>>;
using ShardSymbolRow =
    IndexCollector<cl::ShardSymbolRow,
                   IndexCollectorArg<key_id::SHARD_SYMBOL_PROVIDER,
                                     Member<&cl::ShardSymbolRow::Provider>>,
                   IndexCollectorArg<key_id::SHARD_SYMBOL_ID,
                                     Chained<Member<&cl::ShardSymbolRow::Data>,
                                             Member<&cl::Symbol::ID>>>>;
using ShardRefsRow = IndexCollector<
    cl::ShardRefsRow,
    IndexCollectorArg<key_id::SHARD_REFS_PROVIDER,
                      Member<&cl::ShardRefsRow::Provider>>,
    IndexCollectorArg<
        key_id::SHARD_REFS_ID,
        Chained<Member<&cl::ShardRefsRow::Data>,
                Member<&decltype(cl::ShardRefsRow::Data)::first>>>>;
using ShardRelationRow = IndexCollector<
    cl::ShardRelationRow,
    IndexCollectorArg<key_id::SHARD_RELATION_PROVIDER,
                      Member<&cl::ShardRelationRow::Provider>>,
    IndexCollectorArg<key_id::SHARD_RELATION_KEY,
                      Composite<cl::ShardRelationRow,
                                Chained<Member<&cl::ShardRelationRow::Data>,
                                        Member<&cl::Relation::Subject>>,
                                Chained<Member<&cl::ShardRelationRow::Data>,
                                        Member<&cl::Relation::Predicate>>>>>;

struct IndexedSymbol {
  using Arguments =
      std::tuple<IndexCollectorArg<key_id::INDEXEDSYMBOL_ID,
                                   cl::SymbolID(const cl::IndexedSymbol &)>,
                 IndexCollectorArg<key_id::INDEXEDSYMBOL_TOKEN,
                                   uint64_t(const cl::IndexedSymbol &)>>;
  static constexpr std::size_t IndexCount = 2;

  void operator()(const cl::IndexedSymbol &Obj,
                  llvm::function_ref<bool(IndexID, llvm::StringRef)> Callback) {
    bool Ret =
        Callback(key_id::INDEXEDSYMBOL_ID, typeToStringRef(Obj.Symbol.ID));
    if (!Ret)
      return;
    for (const auto &V : Obj.Tokens) {
      Ret = Callback(key_id::INDEXEDSYMBOL_TOKEN, typeToStringRef(V));
      if (!Ret)
        return;
    }
  }
};

} // namespace indexer

// Mark symbols which are can be used for code completion.
extern const Token RestrictedForCodeCompletion;

std::vector<std::string> generateProximityURIs(llvm::StringRef URIPath);

class LMDBInstance : public LMDB {
  class TransactionImpl : public Transaction {
  public:
    struct MarshallingHelper {
      MarshallingHelper() = delete;
      MarshallingHelper(const TransactionImpl *Txn) : Txn(Txn) {}

      cl::SymbolID fromNative(clangd::SymbolID);
      cl::FileDigest fromNative(clangd::FileDigest);
      cl::SymbolLocation::Position
      fromNative(const clangd::SymbolLocation::Position &);
      cl::SymbolLocation fromNative(const clangd::SymbolLocation &);
      cl::Symbol::IncludeHeaderWithReferences
      fromNative(const clangd::Symbol::IncludeHeaderWithReferences &);
      cl::Symbol fromNative(const clangd::Symbol &);
      cl::Ref fromNative(const clangd::Ref &);
      cl::Relation fromNative(const clangd::Relation &);
      cl::IncludeGraphNode fromNative(const clangd::IncludeGraphNode &);
      cl::IncludeGraph fromNative(const clangd::IncludeGraph &);
      cl::CompileCommand fromNative(const tooling::CompileCommand &);

      clangd::SymbolID toNative(cl::SymbolID);
      clangd::FileDigest toNative(cl::FileDigest);
      clangd::SymbolLocation::Position
      toNative(const cl::SymbolLocation::Position &);
      clangd::SymbolLocation toNative(const cl::SymbolLocation &);
      clangd::Symbol::IncludeHeaderWithReferences
      toNative(const cl::Symbol::IncludeHeaderWithReferences &);
      clangd::Symbol toNative(const cl::Symbol &);
      clangd::Ref toNative(const cl::Ref &);
      clangd::Ref toNative(const Ref &);
      clangd::Relation toNative(const cl::Relation &);
      clangd::IncludeGraphNode toNative(const cl::IncludeGraphNode &);
      clangd::IncludeGraph toNative(const cl::IncludeGraph &);
      tooling::CompileCommand toNative(const cl::CompileCommand &);

      IndexFileInfo fromIndexFileInfo(const cl::IndexFileInfo *);
      cl::IndexFileInfo toIndexFileInfo(llvm::StringRef, const IndexFileInfo &);
      cl::IndexFileInfo toIndexFileInfo(llvm::StringRef, const IndexFileOut &,
                                        int64_t ModifiedTime);
      cl::IndexFileInfo toEmptyIndexFileInfo(llvm::StringRef);

    private:
      const TransactionImpl *Txn;
    };

    virtual ~TransactionImpl() {};

    static llvm::ErrorOr<std::unique_ptr<TransactionImpl>>
    begin(LMDBInstance &DB, bool RO);

    lmdb::Txn &getTxn() { return Txn; }
    bool isReadOnly() const { return Txn.flags() & MDB_RDONLY; }

    std::error_code commit() && override {
      return mapDBErrorToIndexDBError(Txn.commit());
    }
    void abort() && override { Txn.abort(); }

    MarshallingHelper &getMarshaller() { return Marshaller; }

    llvm::ErrorOr<std::pair<OID, cl::IndexFileInfo>>
    findIndexFileInfo(llvm::StringRef Path);
    std::unique_ptr<IndexFileInfo> getShardInfo(llvm::StringRef ShardPath);
    std::unique_ptr<IncludeGraph>
    getShardIncludeGraph(llvm::StringRef ShardPath);

    llvm::ErrorOr<cl::Symbol> lookupShardsSymbol(SymbolID ID);

    std::optional<cl::IndexedSymbol> findSymbol(SymbolID ID);
    llvm::ErrorOr<DatabaseIndexCursor> findReferences(SymbolID ID);
    llvm::ErrorOr<DatabaseIndexCursor> findRelations(SymbolID ID,
                                                     RelationKind Predicate);

    std::error_code updateSymbol(SymbolID ID);

    llvm::ErrorOr<OID> createIndexFileInfo(llvm::StringRef ShardPath,
                                           const cl::IndexFileInfo *ShardInfo);
    std::error_code updateShard(llvm::StringRef ShardPath,
                                const IndexFileOut *Shard,
                                int64_t ModifiedTime);

  private:
    TransactionImpl(LMDBInstance &DB,
                    std::variant<std::shared_lock<std::shared_mutex>,
                                 std::unique_lock<std::shared_mutex>> &&Lk,
                    lmdb::Txn &&Txn)
        : DB(DB.getInstance()), TxnLk(std::move(Lk)), Txn(std::move(Txn)),
          Marshaller(this) {}

    std::shared_ptr<LMDBInstance> DB;
    std::variant<std::shared_lock<std::shared_mutex>,
                 std::unique_lock<std::shared_mutex>>
        TxnLk;
    lmdb::Txn Txn;
    MarshallingHelper Marshaller;
  };

  friend class LMDBIndexImpl;
  friend std::unique_ptr<SymbolIndex>
  createSymbolIndex(std::shared_ptr<LMDB> DB);

  LMDBInstance(llvm::StringRef Path);

  bool refreshEnvMapSize();

public:
  struct QueryRequest {
    static llvm::ErrorOr<QueryRequest> begin(LMDBInstance &);

    std::unique_ptr<Iterator> iterator(const Token &Tok) const;
    // Constructs BOOST iterators for Path Proximities.
    std::unique_ptr<Iterator> createFileProximityIterator(
        llvm::ArrayRef<std::string> ProximityPaths) const;
    // Constructs BOOST iterators for preferred types.
    std::unique_ptr<Iterator>
    createTypeBoostingIterator(llvm::ArrayRef<std::string> Types) const;

    void lookup(const LookupRequest &Req,
                llvm::function_ref<void(const Symbol &)> Callback) const;

    bool fuzzyFind(const FuzzyFindRequest &Req,
                   llvm::function_ref<void(const Symbol &)> Callback) const;

    bool refs(const RefsRequest &Req,
              llvm::function_ref<void(const Ref &)> Callback) const;

    bool fuzzyFind(const FuzzyFindRequest &Req,
                   llvm::function_ref<void(const Symbol &)> Callback);

    void relations(const RelationsRequest &Req,
                   llvm::function_ref<void(const SymbolID &, const Symbol &)>
                       Callback) const;

  private:
    QueryRequest() : Corpus(0) {};

    std::shared_ptr<LMDBInstance> DB;
    std::unique_ptr<TransactionImpl> Snapshot;
    db_index::Corpus Corpus;
  };

  ~LMDBInstance() override;

  std::shared_ptr<LMDBInstance> getInstance() {
    return std::static_pointer_cast<LMDBInstance>(shared_from_this());
  }

  static std::shared_ptr<LMDBInstance> createInstance(llvm::StringRef Path);

  [[noreturn]] void raiseFatalError(std::error_code EC);

  llvm::ErrorOr<std::unique_ptr<Transaction>>
  beginTransaction(bool RO) override;

  size_t databaseSize() override;
  std::error_code growDatabaseSize(std::optional<size_t> OldSizeHint) override;

  std::unique_ptr<IndexFileInfo>
  getShardInfo(Transaction &TransIn, llvm::StringRef ShardPath) override;
  std::error_code updateShard(Transaction &TransIn, llvm::StringRef ShardPath,
                              const IndexFileOut *Shard,
                              int64_t ModifiedTime) override;

  std::shared_mutex TransactionMu;
  lmdb::Environment Env;
  std::atomic<size_t> EnvMapSize;

  Database<cl::IndexFileInfo, indexer::Shard> ShardDB{"DB_SHARD"};
  Database<cl::ShardSymbolRow, indexer::ShardSymbolRow> ShardSymbolsDB{
      "DB_SHARD_SYMBOLS"};
  Database<cl::ShardRefsRow, indexer::ShardRefsRow> ShardRefsDB{
      "DB_SHARD_REFS"};
  Database<cl::ShardRelationRow, indexer::ShardRelationRow> ShardRelationsDB{
      "DB_SHARD_RELATIONS"};
  Database<cl::IndexedSymbol, indexer::IndexedSymbol> IndexedSymbolsDB{
      "DB_INDEXEDSYMBOLS"};

  std::vector<DatabaseMethods *> Databases = {
      &ShardDB,          &ShardSymbolsDB,   &ShardRefsDB,
      &ShardRelationsDB, &IndexedSymbolsDB,
  };
};

} // namespace db_index
} // namespace clangd
} // namespace clang

#endif