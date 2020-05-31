//===--- DbIndex.h - Dynamic on-disk symbol index. ------- C++-*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_DBINDEX_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_DBINDEX_H

#include "../../Path.h"
#include "../Index.h"
#include "../Serialization.h"
#include "Iterator.h"
#include "LmdbCxx.h"
#include "Token.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/Signals.h"
#include <shared_mutex>

namespace clang {
namespace clangd {
namespace dbindex {

/// This class wraps an error for LMDB database
class DbIndexError final : public llvm::ErrorInfo<DbIndexError> {
public:
  /// Error Code
  enum ErrorCode {
    UNKNOWN = 1,
    NOT_FOUND,
    DB_ERROR,
  };

  /// Used by ErrorInfo::classID.
  static char ID;

  /// Construct an error with origin and return code
  DbIndexError(ErrorCode EC, std::string DetailsMsg = "")
      : EC(EC), DetailsMsg(DetailsMsg) {}

  /// Log the error to output stream
  void log(llvm::raw_ostream &OS) const override {
    OS << strerror();
    if (!DetailsMsg.empty())
      OS << ' ' << '(' << DetailsMsg << ')';
  }

  /// Convert the error to std::error_code
  std::error_code convertToErrorCode() const override {
    return llvm::inconvertibleErrorCode();
  }

  /// Get message related to the error
  const char *strerror() const;

  /// Get error code
  ErrorCode errorCode() const { return EC; }

private:
  /// Error code
  ErrorCode EC;

  std::string DetailsMsg;
};

/// \brief Indexing Database based on LMDB
class LMDBIndex {
  /// \brief Names for databases
  static const char *NameDBILog;
  static const char *NameDBIShards;
  static const char *NameDBISymbolIDToShards;
  static const char *NameDBISymbolIDToRefShards;
  static const char *NameDBISymbolIDToRelationShards;

  static const char *NameDBIDocIDFreelist;
  static const char *NameDBIDocIDToSymbols;
  static const char *NameDBISymbolIDToDocID;
  static const char *NameDBIPostingList;

  static constexpr int DBINameNumbers = 9;

  /// \brief Friends
  friend class DbIndex;

public:
  /// \brief opens an LMDB Index at \p Path
  static std::unique_ptr<LMDBIndex> open(PathRef Path);

  /// \brief get shard with corresponding \p FilePath
  llvm::Expected<IndexFileIn> get(llvm::StringRef FilePath);

  /// \brief update interface for a file
  llvm::Error update(llvm::StringRef FilePath, const IndexFileOut &Shard);

  /// \brief build SymbolIndex
  llvm::Expected<std::unique_ptr<SymbolIndex>> buildIndex();

private:
  llvm::Error dbenvMapsizeHandle(llvm::Error Err);

  llvm::Error doLmdbWorkWithResize(llvm::function_ref<llvm::Error()> Fn);

  llvm::Expected<DocID> allocDocID(lmdb::Txn &Txn, lmdb::DBI &DBIFl,
                                   lmdb::DBI &DBI);

  llvm::Error freeDocID(lmdb::Txn &Txn, lmdb::DBI &DBIFl, lmdb::DBI &DBI,
                        DocID DID);

  /// \brief remove Symbol with given SymbolID and the associations with it from
  /// database
  llvm::Error removeSymbolFromStore(lmdb::Txn &Txn, SymbolID ID);

  /// \brief update Symbol with given SymbolID and the associations with it if
  /// not already existed in database
  llvm::Error updateSymbolToStore(lmdb::Txn &Txn, Symbol &S);

  /// \brief update SymbolID to shard pointers for Symbols, Refs and Relations
  llvm::Error updateShardReferences(lmdb::Txn &Txn, llvm::StringRef SourceFile,
                                    IndexFileIn *PrevShard,
                                    const IndexFileOut &CurrShard);

  /// \brief get Symbol with a specified SymbolID from all the shards having it.
  /// Perform merging of decl/defs in place
  std::error_code getSymbolFromShards(lmdb::Txn &Txn, SymbolID ID,
                                      llvm::Optional<Symbol> &Sym);

  /// \brief updates in the unit of a file
  llvm::Error updateFile(llvm::StringRef SourceFile, const IndexFileOut &Shard);

  /// \brief build all index
  llvm::Error buildAllIndex(lmdb::Txn &Txn);

  /// \brief get Symbol with a specified SymbolID. Perform merging of decl/defs
  /// in place
  llvm::Expected<Symbol> getSymbol(lmdb::Txn &Txn, SymbolID ID);

  /// \brief get Symbol with a specified DocID. Perform merging of decl/defs
  /// in place
  llvm::Expected<Symbol> getSymbol(lmdb::Txn &Txn, DocID DID);

  /// Environment handle
  lmdb::Env DBEnv;
  /// SymbolIDs waiting to be indexed and stored in Symbol store
  lmdb::DBI DBILog;
  /// Shards database handle
  lmdb::DBI DBIShards;
  /// Shards-related database handles
  lmdb::DBI DBISymbolIDToShards;
  lmdb::DBI DBISymbolIDToRefShards;
  lmdb::DBI DBISymbolIDToRelationShards;
  /// Symbol database handles
  lmdb::DBI DBIDocIDFreelist;
  lmdb::DBI DBIDocIDToSymbols;
  lmdb::DBI DBISymbolIDToDocID;
  lmdb::DBI DBIPostingList;
  /// Database size upgrade lock
  std::shared_timed_mutex DBEnvMapsizeMutex;
};

/// \brief SymbolIndex interface exported from Indexing database
class DbIndex : public SymbolIndex {
public:
  DbIndex(LMDBIndex *DBIndex, size_t CorpusSize)
      : DBIndex(DBIndex), Corpus(CorpusSize) {}

  bool
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback) const override;

  void lookup(const LookupRequest &Req,
              llvm::function_ref<void(const Symbol &)> Callback) const override;

  bool refs(const RefsRequest &Req,
            llvm::function_ref<void(const Ref &)> Callback) const override;

  void relations(const RelationsRequest &Req,
                 llvm::function_ref<void(const SymbolID &, const Symbol &)>
                     Callback) const override;

  size_t estimateMemoryUsage() const override { return 0; };

private:
  friend class LMDBIndex;

  std::unique_ptr<Iterator> getIterator(lmdb::Txn &Txn, const Token &Tok) const;

  std::unique_ptr<Iterator>
  createFileProximityIterator(lmdb::Txn &Txn,
                              llvm::ArrayRef<std::string> ProximityPaths) const;

  std::unique_ptr<Iterator>
  createTypeBoostingIterator(lmdb::Txn &Txn,
                             llvm::ArrayRef<std::string> Types) const;

  LMDBIndex *DBIndex;
  dbindex::Corpus Corpus;
};

} // namespace dbindex
} // namespace clangd
} // namespace clang

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_DBINDEX_H
