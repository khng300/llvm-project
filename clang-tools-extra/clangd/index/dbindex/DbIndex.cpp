//===--- DbIndex.cpp - Dynamic on-disk symbol index. ------C++-*-===//

#include "DbIndex.h"
#include "Container.h"
#include "FuzzyMatch.h"
#include "LMDBIterator.h"
#include "Logger.h"
#include "Quality.h"
#include "Trigram.h"
#include "URI.h"
#include "index/Merge.h"
#include "index/dbindex/LmdbCxx.h"
#include "index/dbindex/xxhash.h"
#include "llvm/ADT/StringExtras.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/FileSystem.h"
#include <cassert>
#include <lmdb.h>

namespace clang {
namespace clangd {
namespace dbindex {

/// Class ID for DbIndexError.
/// Actually isA() is implemented by comparing address of ErrorInfo::ID in LLVM
char DbIndexError::ID = 0;

const char *DbIndexError::strerror() const {
  switch (EC) {
  case NOT_FOUND:
    return "Not found";
  case DB_ERROR:
    return "DB error";
  default:
    break;
  }

  return "Unknown";
}

/// Names for allocators and databases
#define STRINGIFY(s) (#s)
#define FILL_NAMEDBI(x) const char *LMDBIndex::Name##x = STRINGIFY(x);

FILL_NAMEDBI(DBILog)
FILL_NAMEDBI(DBIShards)
FILL_NAMEDBI(DBISymbolIDToShards)
FILL_NAMEDBI(DBISymbolIDToRefShards)
FILL_NAMEDBI(DBISymbolIDToRelationShards)

FILL_NAMEDBI(DBIDocIDFreelist)
FILL_NAMEDBI(DBISymbolIDToDocID)
FILL_NAMEDBI(DBIDocIDToSymbols)
FILL_NAMEDBI(DBIPostingList)

#undef FILL_NAMEDBI
#undef STRINGIFY

/// Hash Subject-Predicate
static uint64_t makeSubjectPredicateHash(SymbolID Subject, uint8_t Predicate) {
  return dbindex::xxHash64(dbindex::XxHashInitialSeed, Subject.raw(),
                           Predicate);
}

static uint64_t makeStringHash(llvm::StringRef S) {
  return dbindex::xxHash64(dbindex::XxHashInitialSeed, S);
}

llvm::Error LMDBIndex::dbenvMapsizeHandle(llvm::Error Err) {
  return llvm::handleErrors(
      std::move(Err), [this](lmdb::Error &IndexErr) -> llvm::Error {
        if (IndexErr.errorValue() == MDB_MAP_FULL) {
          std::unique_lock<std::shared_timed_mutex> Lock(DBEnvMapsizeMutex);
          auto EnvInfo = DBEnv.envInfo();
          if (!EnvInfo)
            return EnvInfo.takeError();
          if (auto Err = DBEnv.setMapsize(EnvInfo->me_mapsize << 1))
            return Err;
          return llvm::Error::success();
        } else if (IndexErr.errorValue() == MDB_MAP_RESIZED) {
          if (auto Err = DBEnv.setMapsize(0))
            return Err;
          return llvm::Error::success();
        }

        return llvm::make_error<DbIndexError>(DbIndexError::DB_ERROR,
                                              IndexErr.message());
      });
}

llvm::Error
LMDBIndex::doLmdbWorkWithResize(llvm::function_ref<llvm::Error()> Fn) {
  auto WrappedFn = [&Fn, this]() -> llvm::Error {
    std::shared_lock<std::shared_timed_mutex> Lock(DBEnvMapsizeMutex);
    return Fn();
  };
  while (auto Err = WrappedFn()) {
    Err = dbenvMapsizeHandle(std::move(Err));
    if (Err)
      return Err;
  }
  return llvm::Error::success();
}

llvm::Expected<std::unique_ptr<LMDBIndex>> LMDBIndex::open(PathRef Path) {
  // If the database environment directory exists we
  // do not need a new one
  if (auto EC = llvm::sys::fs::create_directory(Path, true))
    return llvm::errorCodeToError(EC);
  llvm::Expected<lmdb::Env> Env = lmdb::Env::create();
  if (!Env) {
    return Env.takeError();
  }

  // Now we need this number of databases for our use including 1 allocator
  auto Err = Env->setMaxDBs(LMDBIndex::DBINameNumbers);
  if (Err)
    return std::move(Err);
  // No metasync during open as we do not need durability
  Err = Env->open(Path, MDB_NOMETASYNC);
  if (Err)
    return std::move(Err);

  auto LMDBIndexPtr = std::make_unique<LMDBIndex>();
  auto DoWork = [&]() -> llvm::Error {
    // Now starts a transaction so that we can persist changes to database,
    // i.e. creation of ID allocators and databases
    llvm::Expected<lmdb::Txn> Txn = lmdb::Txn::begin(*Env);
    if (!Txn)
      return Txn.takeError();

#define OPEN_DBI(x, __flags)                                                   \
  llvm::Expected<lmdb::DBI> x =                                                \
      lmdb::DBI::open(*Txn, LMDBIndex::Name##x, (__flags));                    \
  if (!(x))                                                                    \
    return (x).takeError();

    OPEN_DBI(DBILog, MDB_CREATE);
    OPEN_DBI(DBIShards, MDB_CREATE);
    OPEN_DBI(DBISymbolIDToShards, MDB_CREATE | MDB_DUPSORT);
    OPEN_DBI(DBISymbolIDToRefShards, MDB_CREATE | MDB_DUPSORT);
    OPEN_DBI(DBISymbolIDToRelationShards, MDB_CREATE | MDB_DUPSORT);

    OPEN_DBI(DBIDocIDFreelist, MDB_CREATE | MDB_INTEGERKEY);
    OPEN_DBI(DBIDocIDToSymbols, MDB_CREATE | MDB_INTEGERKEY);
    OPEN_DBI(DBISymbolIDToDocID, MDB_CREATE);
    OPEN_DBI(DBIPostingList, MDB_CREATE | MDB_DUPSORT | MDB_INTEGERDUP);
#undef OPEN_DBI

    // Commit the changes to storage in one go
    Err = Txn->commit();
    if (Err)
      return std::move(Err);

    LMDBIndexPtr->DBEnv = std::move(*Env);
#define FILL_INDEX(x) LMDBIndexPtr->x = std::move(*(x))
    FILL_INDEX(DBILog);
    FILL_INDEX(DBIShards);
    FILL_INDEX(DBISymbolIDToShards);
    FILL_INDEX(DBISymbolIDToRefShards);
    FILL_INDEX(DBISymbolIDToRelationShards);

    FILL_INDEX(DBIDocIDFreelist);
    FILL_INDEX(DBIDocIDToSymbols);
    FILL_INDEX(DBISymbolIDToDocID);
    FILL_INDEX(DBIPostingList);
#undef FILL_INDEX

    return llvm::Error::success();
  };

  if (auto Err = LMDBIndexPtr->doLmdbWorkWithResize(DoWork))
    return std::move(Err);

  return LMDBIndexPtr;
}

llvm::Expected<DocID> LMDBIndex::allocDocID(lmdb::Txn &Txn, lmdb::DBI &DBIFl,
                                            lmdb::DBI &DBI) {
  auto Cursor = lmdb::Cursor::open(Txn, DBIFl);
  if (!Cursor)
    return Cursor.takeError();

  DocID DID = -1ull;
  lmdb::Val K, D;
  auto Res = Cursor->get(K, D, MDB_NEXT);
  if (!Res)
    return Res.takeError();

  if (Res.isHit()) {
    DID = *K.data<DocID>();
    Res = Cursor->del();
    if (!Res)
      return Res.takeError();
  } else {
    auto Stat = DBI.stat(Txn);
    if (!Stat)
      return Stat.takeError();
    DID = Stat->ms_entries;
    Res = DBI.put(Txn, lmdb::Val(&DID), lmdb::Val(), 0);
    if (!Res)
      return Res.takeError();
  }

  return DID;
}

llvm::Error LMDBIndex::freeDocID(lmdb::Txn &Txn, lmdb::DBI &DBIFl,
                                 lmdb::DBI &DBI, DocID DID) {
  auto Res = DBIFl.put(Txn, lmdb::Val(&DID), lmdb::Val(), 0);
  if (!Res)
    return Res.takeError();
  Res = DBI.put(Txn, lmdb::Val(&DID), lmdb::Val(), 0);
  return Res.takeError();
}

llvm::Expected<IndexFileIn> LMDBIndex::get(llvm::StringRef FilePath) {
  IndexFileIn IFile;
  auto DoWork = [&]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBEnv, nullptr, MDB_RDONLY);
    if (!Txn)
      return Txn.takeError();

    lmdb::Val ShardContent;
    auto Res = DBIShards.get(*Txn, lmdb::Val(digest(FilePath)), ShardContent);
    if (!Res)
      return Res.takeError();
    if (Res.isMiss())
      return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
    auto Ret = container::readContainer(ShardContent);
    if (!Ret)
      return Ret.takeError();
    if (auto Err = Txn->commit())
      return Err;

    IFile = std::move(*Ret);
    return llvm::Error::success();
  };

  if (auto Err = doLmdbWorkWithResize(DoWork))
    return std::move(Err);
  return IFile;
}

llvm::Error LMDBIndex::update(llvm::StringRef FilePath,
                              const IndexFileOut &Shard) {
  const auto TimerStart = std::chrono::high_resolution_clock::now();
  auto Err = updateFile(FilePath, Shard);
  const auto TimerStop = std::chrono::high_resolution_clock::now();
  const auto Duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      TimerStop - TimerStart);
  vlog("Update of {0} took {1}. Result: {2}. Indexed ({3} symbols, {4} "
       "refs)",
       FilePath, Duration, Err, Shard.Symbols ? Shard.Symbols->size() : 0,
       Shard.Refs ? Shard.Refs->numRefs() : 0);
  return Err;
}

llvm::Expected<std::unique_ptr<SymbolIndex>> LMDBIndex::buildIndex() {
  size_t SymbolDBSize = -1;
  auto DoWork = [this, &SymbolDBSize]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBEnv);
    if (!Txn)
      return Txn.takeError();

    if (auto Err = buildAllIndex(*Txn))
      return Err;

    auto Stat = DBIDocIDToSymbols.stat(*Txn);
    if (!Stat)
      return Stat.takeError();
    SymbolDBSize = Stat->ms_entries;

    // The final step is to commit all the changes made
    return Txn->commit();
  };

  const auto TimerStart = std::chrono::high_resolution_clock::now();
  auto Err = doLmdbWorkWithResize(DoWork);
  const auto TimerStop = std::chrono::high_resolution_clock::now();
  const auto Duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      TimerStop - TimerStart);
  log("Index update took {0}. Result: {1}.", Duration, Err);

  if (Err)
    return std::move(Err);
  return std::make_unique<DbIndex>(this, SymbolDBSize);
}

llvm::Error LMDBIndex::removeSymbolFromStore(lmdb::Txn &Txn, SymbolID ID) {
  lmdb::Val V;
  auto Res = DBISymbolIDToDocID.get(Txn, ID.raw(), V);
  if (!Res)
    return Res.takeError();
  if (Res.isMiss())
    return llvm::Error::success();

  DocID DID = *V.data<DocID>();
  Res = DBIDocIDToSymbols.get(Txn, lmdb::Val(&DID), V);
  if (!Res)
    return Res.takeError();
  if (Res.isMiss())
    return llvm::Error::success();

  Symbol S = container::readSymbol(V);
  // Remove search tokens corresponding to the Symbol from DBITrigramToDocID
  for (auto &Token : generateSearchTokens(S)) {
    uint64_t Hash = Token.getPersistentHash();
    Res = DBIPostingList.del(Txn, lmdb::Val(&Hash), lmdb::Val(&DID));
    if (!Res)
      return Res.takeError();
  }
  // Remove the SymbolID to DocID mapping
  Res = DBISymbolIDToDocID.del(Txn, ID.raw(), llvm::None);
  if (!Res)
    return Res.takeError();
  if (Res.isMiss())
    return llvm::Error::success();

  // Free the DocID of the Symbol
  return freeDocID(Txn, DBIDocIDFreelist, DBIDocIDToSymbols, DID);
}

llvm::Error LMDBIndex::updateSymbolToStore(lmdb::Txn &Txn, Symbol &S) {
  // Generate trigram tokens corresponding to the unqualified name of
  // the symbol. Then, insert trigram tokens to SymbolID associations.

  // Check whether the Symbol exists in SymbolID -> Symbol database
  // If the Symbol exists in the database, skip the process of updating
  // trigrams and scope of the corresponding Symbol

  DocID DID = -1;
  lmdb::Val V;
  auto Res = DBISymbolIDToDocID.get(Txn, S.ID.raw(), V);
  if (!Res)
    return Res.takeError();
  if (Res.isHit()) {
    DID = *V.data<DocID>();
    Res = DBIDocIDToSymbols.get(Txn, lmdb::Val(&DID), V);
    if (!Res)
      return Res.takeError();
    if (Res.isMiss())
      return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
    Symbol Sym = container::readSymbol(V);
    // Remove search tokens corresponding to the Symbol from DBITrigramToDocID
    for (auto &Token : generateSearchTokens(Sym)) {
      uint64_t Hash = Token.getPersistentHash();
      Res = DBIPostingList.del(Txn, lmdb::Val(&Hash), lmdb::Val(&DID));
      if (!Res)
        return Res.takeError();
    }
  } else {
    if (auto E = allocDocID(Txn, DBIDocIDFreelist, DBIDocIDToSymbols))
      DID = *E;
    else
      return E.takeError();

    Res = DBISymbolIDToDocID.put(Txn, S.ID.raw(), lmdb::Val(&DID), 0);
    if (!Res)
      return Res.takeError();
  }

  // Insert search tokens of the symbol
  for (const auto &Token : generateSearchTokens(S)) {
    uint64_t Hash = Token.getPersistentHash();
    Res = DBIPostingList.put(Txn, lmdb::Val(&Hash), lmdb::Val(&DID),
                             MDB_NODUPDATA);
    if (!Res)
      return Res.takeError();
  }

  // Update DBIDocIDToSymbols database
  std::string SS = container::writeSymbol(S);
  V = SS;
  Res = DBIDocIDToSymbols.put(Txn, lmdb::Val(&DID), V, 0);
  if (!Res)
    return Res.takeError();

  return llvm::Error::success();
}

llvm::Error LMDBIndex::updateShardReferences(lmdb::Txn &Txn,
                                             llvm::StringRef SourceFile,
                                             IndexFileIn *PrevShard,
                                             const IndexFileOut &CurrShard) {
  // Current we use hashed \p FilePath as file ID.
  uint64_t FileID = makeStringHash(SourceFile);
  auto InsertLog = [this, &Txn](SymbolID ID) -> llvm::Error {
    auto Res = DBILog.put(Txn, ID.raw(), lmdb::Val(), MDB_NOOVERWRITE);
    if (!Res)
      return Res.takeError();
    return llvm::Error::success();
  };

  if (PrevShard) {
    // Delete all the \p SS related to this file
    if (PrevShard->Symbols) {
      for (const auto &S : *PrevShard->Symbols) {
        auto Res = DBISymbolIDToShards.del(Txn, S.ID.raw(), lmdb::Val(&FileID));
        if (!Res)
          return Res.takeError();
        if (Res.isMiss())
          return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);

        if (auto Err = InsertLog(S.ID))
          return Err;
      }
    }
    // Delete all the \p RS related to this file
    if (PrevShard->Refs) {
      for (const auto &RP : *PrevShard->Refs) {
        SymbolID ID = RP.first;
        auto Res =
            DBISymbolIDToRefShards.del(Txn, ID.raw(), lmdb::Val(&FileID));
        if (!Res)
          return Res.takeError();
        if (Res.isMiss())
          return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
      }
    }
    // Delete all \p Relation related to this file
    if (PrevShard->Relations) {
      llvm::DenseSet<std::pair<SymbolID, uint8_t>> Set;
      for (const auto &R : *PrevShard->Relations) {
        Set.insert({R.Subject, static_cast<uint8_t>(R.Predicate)});
      }
      for (const auto &R : Set) {
        auto SPHash = makeSubjectPredicateHash(R.first, R.second);
        auto Res = DBISymbolIDToRelationShards.del(Txn, lmdb::Val(&SPHash),
                                                   lmdb::Val(&FileID));
        if (!Res)
          return Res.takeError();
        if (Res.isMiss())
          return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
      }
    }
  }
  if (PrevShard && !PrevShard->Sources) {
    // IF no Sources is given, that implies the file is removed.
    if (PrevShard) {
      auto Res = DBIShards.del(Txn, lmdb::Val(&FileID), llvm::None);
      if (!Res)
        return Res.takeError();
      if (Res.isMiss())
        return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
    }
    return Txn.commit();
  }

  std::string ShardContent;
  {
    llvm::raw_string_ostream OS(ShardContent);
    container::writeContainer(CurrShard, OS);
  }
  // Put serialized shard into shard database
  auto Res = DBIShards.put(Txn, lmdb::Val(&FileID), ShardContent, 0);
  if (!Res)
    return Res.takeError();
  if (CurrShard.Symbols) {
    // Insert SymbolID -> hashed ShardIdentifier mapping to DBISymbolIDToShards
    for (const auto &S : *CurrShard.Symbols) {
      Res = DBISymbolIDToShards.put(Txn, S.ID.raw(), lmdb::Val(&FileID), 0);
      if (!Res)
        return Res.takeError();

      if (auto Err = InsertLog(S.ID))
        return Err;
    }
  }
  if (CurrShard.Refs) {
    // Insert SymbolID -> hashed ShardIdentifier mappings to
    // DBISymbolIDToRefShards
    for (const auto &RP : *CurrShard.Refs) {
      SymbolID ID = RP.first;
      Res = DBISymbolIDToRefShards.put(Txn, ID.raw(), lmdb::Val(&FileID), 0);
      if (!Res)
        return Res.takeError();
    }
  }
  if (CurrShard.Relations) {
    // Insert hashed Subject:Predicate -> hashed ShardIdentifier mappings to
    // DBISymbolIDToRelationShards
    llvm::DenseSet<std::pair<SymbolID, uint8_t>> Set;
    for (const auto &R : *CurrShard.Relations) {
      Set.insert({R.Subject, static_cast<uint8_t>(R.Predicate)});
    }
    for (const auto &R : Set) {
      auto SPHash = makeSubjectPredicateHash(R.first, R.second);
      Res = DBISymbolIDToRelationShards.put(Txn, lmdb::Val(&SPHash),
                                            lmdb::Val(&FileID), 0);
      if (!Res)
        return Res.takeError();
    }
  }

  return llvm::Error::success();
}

llvm::Error LMDBIndex::getSymbolFromShards(lmdb::Txn &Txn, SymbolID ID,
                                           llvm::Optional<Symbol> &Sym) {
  auto Cursor = lmdb::Cursor::open(Txn, DBISymbolIDToShards);
  if (!Cursor)
    return Cursor.takeError();

  // Check if the SymbolID still exists. If it does not, remove the
  // inverted index and scope index related to this symbol. If it does,
  // merge the symbol records corresponding to the same SymbolID to provide
  // a Symbol for query.

  lmdb::Val K = ID.raw(), D;
  auto Res = Cursor->get(K, D, MDB_SET);
  for (; Res.isHit(); Res = Cursor->get(K, D, MDB_NEXT_DUP)) {
    auto FID = *D.data<FileDigest>();
    lmdb::Val ShardContent;
    Res = DBIShards.get(Txn, lmdb::Val(FID), ShardContent);
    if (!Res)
      return Res.takeError();
    if (Res.isMiss())
      return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
    auto Result = container::getSymbolInContainer(ShardContent, ID);
    if (!Result)
      return llvm::Error::success();

    Symbol S;
    S = *Result;

    // We must be able to find the symbol in the shard otherwise it
    // indicates inconsistencies.
    if (!Sym)
      Sym = S;
    else
      Sym = mergeSymbol(*Sym, S);
  }

  if (!Res)
    return Res.takeError();
  return llvm::Error::success();
}

llvm::Error LMDBIndex::updateFile(llvm::StringRef SourceFile,
                                  const IndexFileOut &Shard) {
  uint64_t FileID = makeStringHash(SourceFile);

  auto DoWork = [&]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBEnv);
    if (!Txn)
      return Txn.takeError();

    llvm::Optional<IndexFileIn> PrevShard;
    {
      lmdb::Val OldShardContent;
      auto Res = DBIShards.get(*Txn, lmdb::Val(&FileID), OldShardContent);
      if (!Res)
        return Res.takeError();
      if (Res.isHit()) {
        auto IndexFile = container::readContainer(OldShardContent);
        if (!IndexFile)
          return IndexFile.takeError();
        PrevShard = std::move(*IndexFile);
      }
    }
    if (PrevShard && Shard.Sources) {
      bool Modified = false;
      // If digest of old and new shard are the same we skip index updating
      for (auto &I : *PrevShard->Sources) {
        const auto Target = Shard.Sources->find(I.first());
        if (Target == Shard.Sources->end() ||
            Target->second.Digest != I.second.Digest) {
          Modified = true;
          break;
        }
      }
      Modified |= Shard.Sources->size() != PrevShard->Sources->size();
      if (!Modified)
        return llvm::Error::success();
    }

    if (auto Err = updateShardReferences(
            *Txn, SourceFile, PrevShard ? PrevShard.getPointer() : nullptr,
            Shard))
      return Err;

    return Txn->commit();
  };

  return doLmdbWorkWithResize(DoWork);
}

llvm::Error LMDBIndex::buildAllIndex(lmdb::Txn &Txn) {
  auto Stat = DBILog.stat(Txn);
  if (!Stat)
    return Stat.takeError();
  if (!Stat->ms_entries)
    return llvm::Error::success();

  auto Cursor = lmdb::Cursor::open(Txn, DBILog);
  if (!Cursor)
    return Cursor.takeError();

  // Iterate the touched Symbols and see whether the corresponding Symbol in
  // Symbols database should be updated or removed

  lmdb::Val K, D;
  lmdb::Result Res = Cursor->get(K, D, MDB_NEXT);
  for (; Res.isHit(); Res = Cursor->get(K, D, MDB_NEXT)) {
    llvm::Optional<Symbol> Sym;

    SymbolID ID = SymbolID::fromRaw(K);
    auto Err = getSymbolFromShards(Txn, ID, Sym);
    if (Err)
      return Err;

    if (!Sym) {
      // No SymbolID -> hashed ShardIdentifier mapping exists, thus clean up
      // the trigram inverted index and scope index
      if (auto Err = removeSymbolFromStore(Txn, ID))
        return Err;
    } else {
      // There exists SymbolID -> hashed ShardIdentifier mappings, thus update
      // the Symbol in Symbols database
      if (auto Err = updateSymbolToStore(Txn, *Sym))
        return Err;
    }
  }
  if (!Res)
    return Res.takeError();

  return DBILog.drop(Txn);
}

llvm::Expected<Symbol> LMDBIndex::getSymbol(lmdb::Txn &Txn, SymbolID ID) {
  lmdb::Val V;
  auto Res = DBISymbolIDToDocID.get(Txn, ID.raw(), V);
  if (!Res)
    return Res.takeError();
  if (Res.isMiss())
    return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
  return getSymbol(Txn, *V.data<DocID>());
}

llvm::Expected<Symbol> LMDBIndex::getSymbol(lmdb::Txn &Txn, DocID DID) {
  lmdb::Val V;
  auto Res = DBIDocIDToSymbols.get(Txn, lmdb::Val(&DID), V);
  if (!Res)
    return Res.takeError();
  if (Res.isMiss())
    return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
  return container::readSymbol(V);
}

std::unique_ptr<Iterator> DbIndex::getIterator(lmdb::Txn &Txn,
                                               const Token &Tok) const {
  llvm::Expected<lmdb::Cursor> Cursor =
      lmdb::Cursor::open(Txn, DBIndex->DBIPostingList);
  if (!Cursor)
    return Corpus.none();
  uint64_t Hash = Tok.getPersistentHash();
  lmdb::Val K(&Hash), V;
  auto Res = Cursor->get(K, V, MDB_SET_KEY);
  if (!Res || Res.isMiss()) {
    llvm::consumeError(Res.takeError());
    return Corpus.none();
  }
  return std::make_unique<dbindex::LMDBIterator>(std::move(*Cursor));
}

// Constructs BOOST iterators for Path Proximities. Taken from Dex
std::unique_ptr<Iterator> DbIndex::createFileProximityIterator(
    lmdb::Txn &Txn, llvm::ArrayRef<std::string> ProximityPaths) const {
  std::vector<std::unique_ptr<Iterator>> BoostingIterators;
  // Deduplicate parent URIs extracted from the ProximityPaths.
  llvm::StringSet<> ParentURIs;
  llvm::StringMap<SourceParams> Sources;
  for (const auto &Path : ProximityPaths) {
    Sources[Path] = SourceParams();
    auto PathURI = URI::create(Path);
    const auto PathProximityURIs = generateProximityURIs(PathURI.toString());
    for (const auto &ProximityURI : PathProximityURIs)
      ParentURIs.insert(ProximityURI);
  }
  // Use SymbolRelevanceSignals for symbol relevance evaluation: use defaults
  // for all parameters except for Proximity Path distance signal.
  SymbolRelevanceSignals PathProximitySignals;
  // DistanceCalculator will find the shortest distance from ProximityPaths to
  // any URI extracted from the ProximityPaths.
  URIDistance DistanceCalculator(Sources);
  PathProximitySignals.FileProximityMatch = &DistanceCalculator;
  // Try to build BOOST iterator for each Proximity Path provided by
  // ProximityPaths. Boosting factor should depend on the distance to the
  // Proximity Path: the closer processed path is, the higher boosting factor.
  for (const auto &ParentURI : ParentURIs.keys()) {
    // FIXME(kbobyrev): Append LIMIT on top of every BOOST iterator.
    auto It = getIterator(Txn, Token(Token::Kind::ProximityURI, ParentURI));
    if (It->kind() != Iterator::Kind::False) {
      PathProximitySignals.SymbolURI = ParentURI;
      BoostingIterators.push_back(
          Corpus.boost(std::move(It), PathProximitySignals.evaluate()));
    }
  }
  BoostingIterators.push_back(Corpus.all());
  return Corpus.unionOf(std::move(BoostingIterators));
}

// Constructs BOOST iterators for preferred types. Taken from Dex
std::unique_ptr<Iterator>
DbIndex::createTypeBoostingIterator(lmdb::Txn &Txn,
                                    llvm::ArrayRef<std::string> Types) const {
  std::vector<std::unique_ptr<Iterator>> BoostingIterators;
  SymbolRelevanceSignals PreferredTypeSignals;
  PreferredTypeSignals.TypeMatchesPreferred = true;
  auto Boost = PreferredTypeSignals.evaluate();
  for (const auto &T : Types) {
    BoostingIterators.push_back(
        Corpus.boost(getIterator(Txn, Token(Token::Kind::Type, T)), Boost));
  }
  BoostingIterators.push_back(Corpus.all());
  return Corpus.unionOf(std::move(BoostingIterators));
}

bool DbIndex::fuzzyFind(
    const FuzzyFindRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  bool More = !Req.Query.empty() && Req.Query.size() < 3;

  auto DoWork = [&]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBIndex->DBEnv, nullptr, MDB_RDONLY);
    if (!Txn)
      return Txn.takeError();

    std::vector<std::unique_ptr<Iterator>> Criteria;
    FuzzyMatcher Filter(Req.Query);

    std::vector<std::unique_ptr<Iterator>> TrigramIterators;
    for (const auto &Token : generateQueryTrigrams(Req.Query)) {
      TrigramIterators.push_back(getIterator(*Txn, Token));
    }
    Criteria.push_back(Corpus.intersect(std::move(TrigramIterators)));

    std::vector<std::unique_ptr<Iterator>> ScopeIterators;
    for (const auto &Scope : Req.Scopes) {
      ScopeIterators.push_back(
          getIterator(*Txn, Token(Token::Kind::Scope, Scope)));
    }
    if (Req.AnyScope)
      ScopeIterators.push_back(
          Corpus.boost(Corpus.all(), ScopeIterators.empty() ? 1.0 : 0.2));
    Criteria.push_back(Corpus.unionOf(move(ScopeIterators)));

    Criteria.push_back(createFileProximityIterator(*Txn, Req.ProximityPaths));
    Criteria.push_back(createTypeBoostingIterator(*Txn, Req.PreferredTypes));
    if (Req.RestrictForCodeCompletion)
      Criteria.push_back(getIterator(*Txn, RestrictedForCodeCompletion));

    auto Root = Corpus.intersect(move(Criteria));
    if (Req.Limit)
      Root = Corpus.limit(move(Root), *Req.Limit * 100);

    using IDAndScore = std::pair<Symbol, float>;
    auto Compare = [](const IDAndScore &LHS, const IDAndScore &RHS) {
      return LHS.second > RHS.second;
    };

    TopN<IDAndScore, decltype(Compare)> Top(
        Req.Limit ? *Req.Limit : std::numeric_limits<size_t>::max(), Compare);
    for (; !Root->reachedEnd(); Root->advance()) {
      auto DID = Root->peek();
      auto BoostingScore = Root->consume();
      auto Sym = DBIndex->getSymbol(*Txn, DID);
      if (!Sym) {
        return Sym.takeError();
      }
      auto Score = Filter.match(Sym->Name);
      if (Score)
        More |= Top.push({*Sym, *Score * quality(*Sym) * BoostingScore});
    }
    for (const auto &I : std::move(Top).items()) {
      Callback(I.first);
    }

    return Txn->commit();
  };

  auto Err = DBIndex->doLmdbWorkWithResize(DoWork);
  if (Err) {
    llvm::consumeError(std::move(Err));
    return false;
  }

  return More;
}

void DbIndex::lookup(const LookupRequest &Req,
                     llvm::function_ref<void(const Symbol &)> Callback) const {
  auto DoWork = [&]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBIndex->DBEnv, nullptr, MDB_RDONLY);
    if (!Txn)
      return Txn.takeError();

    for (const auto &ID : Req.IDs) {
      auto Sym = DBIndex->getSymbol(*Txn, ID);
      if (!Sym)
        return Sym.takeError();

      Callback(*Sym);
    }

    return Txn->commit();
  };

  llvm::consumeError(DBIndex->doLmdbWorkWithResize(DoWork));
}

bool DbIndex::refs(const RefsRequest &Req,
                   llvm::function_ref<void(const Ref &)> Callback) const {
  bool More = false;
  uint32_t Remaining =
      Req.Limit.getValueOr(std::numeric_limits<uint32_t>::max());

  auto DoWork = [&]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBIndex->DBEnv, nullptr, MDB_RDONLY);
    if (!Txn)
      return Txn.takeError();

    for (const auto &ID : Req.IDs) {
      llvm::Expected<lmdb::Cursor> Cursor =
          lmdb::Cursor::open(*Txn, DBIndex->DBISymbolIDToRefShards);
      if (!Cursor)
        return Cursor.takeError();

      lmdb::Val K(ID.raw()), D;
      auto Res = Cursor->get(K, D, MDB_SET);
      if (!Res)
        return Res.takeError();
      if (Res.isMiss())
        continue;

      do {
        lmdb::Val ShardContent;
        Res = DBIndex->DBIShards.get(*Txn, D, ShardContent);
        if (!Res)
          return Res.takeError();
        if (Res.isMiss())
          continue;
        auto Refs = container::getRefsInContainer(ShardContent, ID);
        for (const auto &I : *Refs) {
          if (!Remaining--) {
            More = true;
            return llvm::Error::success();
          }
          Callback(I);
        }
      } while ((Res = Cursor->get(K, D, MDB_NEXT_DUP)) && Res.isHit());
      if (!Res)
        return Res.takeError();
    }

    return Txn->commit();
  };

  auto Err = DBIndex->doLmdbWorkWithResize(DoWork);
  if (Err) {
    llvm::consumeError(std::move(Err));
    return false;
  }

  return More;
}

void DbIndex::relations(
    const RelationsRequest &Req,
    llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback) const {
  auto DoWork = [&]() -> llvm::Error {
    auto Txn = lmdb::Txn::begin(DBIndex->DBEnv, nullptr, MDB_RDONLY);
    if (!Txn)
      return Txn.takeError();

    for (const auto &Subject : Req.Subjects) {
      llvm::Expected<lmdb::Cursor> Cursor =
          lmdb::Cursor::open(*Txn, DBIndex->DBISymbolIDToRelationShards);
      if (!Cursor)
        return Cursor.takeError();

      LookupRequest LookupReq;
      auto SPHash = makeSubjectPredicateHash(
          Subject, static_cast<uint8_t>(Req.Predicate));
      std::vector<SymbolID> Objects;

      lmdb::Val K(&SPHash), D;
      auto Res = Cursor->get(K, D, MDB_SET);
      if (!Res)
        return Res.takeError();
      if (Res.isMiss())
        continue;

      do {
        lmdb::Val ShardContent;
        Res = DBIndex->DBIShards.get(*Txn, D, ShardContent);
        if (!Res)
          return Res.takeError();
        if (Res.isMiss())
          return llvm::make_error<DbIndexError>(DbIndexError::NOT_FOUND);
        container::getRelationsInContainer(ShardContent, Subject, Req.Predicate,
                                           [&](const Relation &Rel) {
                                             LookupReq.IDs.insert(Rel.Object);
                                             return true;
                                           });
      } while ((Res = Cursor->get(K, D, MDB_NEXT_DUP)) && Res.isHit());
      if (!Res)
        return Res.takeError();

      for (const auto &ID : LookupReq.IDs) {
        auto Sym = DBIndex->getSymbol(*Txn, ID);
        if (!Sym)
          return Sym.takeError();

        Callback(Subject, *Sym);
      }
    }

    return Txn->commit();
  };

  llvm::consumeError(DBIndex->doLmdbWorkWithResize(DoWork));
}

} // namespace dbindex
} // namespace clangd
} // namespace clang
