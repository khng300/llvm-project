//===--- DocumentStore.cpp - Symbol database --------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "DocumentStore.h"
#include "FuzzyMatch.h"
#include "Merge.h"
#include "Quality.h"
#include "SymbolIndex.h"
#include "Trigram.h"
#include "support/Logger.h"
#include "llvm/Support/FileSystem.h"

#include <optional>
#include <system_error>

namespace clang {
namespace clangd {
namespace db_index {

namespace {

// Helper to efficiently assemble the inverse index (token -> matching docs).
// The output is a nice uniform structure keyed on Token, but constructing
// the Token object every time we want to insert into the map is wasteful.
// Instead we have various maps keyed on things that are cheap to compute,
// and produce the Token keys once at the end.
std::vector<Token> buildTokens(const cl::Symbol &Sym) {
  llvm::DenseSet<Trigram> TrigramDocs;
  bool RestrictedCCDocs = false;
  llvm::StringSet<> TypeDocs;
  llvm::StringSet<> ScopeDocs;
  llvm::StringSet<> ProximityDocs;
  std::vector<Trigram> TrigramScratch;
  // Add the tokens which are given symbol's characteristics.
  // This includes fuzzy matching trigrams, symbol's scope, etc.
  // FIXME(kbobyrev): Support more token types:
  // * Namespace proximity
  [&](const cl::Symbol &Sym) {
    generateIdentifierTrigrams(std::string_view(Sym.Name), TrigramScratch);
    for (Trigram T : TrigramScratch)
      TrigramDocs.insert(T);
    ScopeDocs.insert(std::string_view(Sym.Scope));
    if (!Sym.CanonicalDeclaration.FileURI.empty())
      for (const auto &ProximityURI : generateProximityURIs(
               std::string_view(Sym.CanonicalDeclaration.FileURI))) {
        ProximityDocs.insert(ProximityURI);
      }
    if (Sym.Flags & Symbol::IndexedForCodeCompletion)
      RestrictedCCDocs = true;
    if (!Sym.Type.empty())
      TypeDocs.insert(std::string_view(Sym.Type));
  }(Sym);

  std::vector<Token> Result;
  Result.reserve(/*InitialReserve=*/
                 TrigramDocs.size() + (RestrictedCCDocs ? 1 : 0) +
                 TypeDocs.size() + ScopeDocs.size() + ProximityDocs.size());
  // Tear down intermediate structs as we go to reduce memory usage.
  // Since we're trying to get rid of underlying allocations, clearing the
  // containers is not enough.
  auto CreatePostingList = [&Result](Token::Kind TK, llvm::StringSet<> &Docs) {
    for (auto &E : Docs)
      Result.emplace_back(Token(TK, E.first()));
  };
  CreatePostingList(Token::Kind::Type, TypeDocs);
  CreatePostingList(Token::Kind::Scope, ScopeDocs);
  CreatePostingList(Token::Kind::ProximityURI, ProximityDocs);

  // TrigramDocs are stored in a DenseMap and RestrictedCCDocs is not even a
  // map, treat them specially.
  for (auto &E : TrigramDocs)
    Result.emplace_back(Token(Token::Kind::Trigram, E.view()));
  if (RestrictedCCDocs)
    Result.emplace_back(RestrictedForCodeCompletion);

  return Result;
}

/// Implements iterator of PostingList chunks. This requires iterating over two
/// levels: the first level iterator iterates over the chunks and decompresses
/// them on-the-fly when the contents of chunk are to be seen.
class IndexIterator : public Iterator {
public:
  explicit IndexIterator(DatabaseIndexCursor &&Cursor)
      : IndexCursor(std::move(Cursor)) {}

  bool reachedEnd() const override { return IsEnd; }

  /// Advances cursor to the next item.
  void advance() override {
    assert(!reachedEnd() &&
           "Posting List iterator can't advance() at the end.");
    auto OidOrErr = IndexCursor.next();
    IsEnd = !(OidOrErr && *OidOrErr != EmptyOid);
  }

  /// Applies binary search to advance cursor to the next item with DocID
  /// equal or higher than the given one.
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() &&
           "Posting List iterator can't advance() at the end.");
    if (ID <= peek())
      return;
    auto OidOrErr = IndexCursor.gotoOID(ID);
    IsEnd = !(OidOrErr && *OidOrErr != EmptyOid);
  }

  DocID peek() const override {
    assert(!reachedEnd() && "Posting List iterator can't peek() at the end.");
    return IndexCursor.getOID();
  }

  float consume() override {
    assert(!reachedEnd() &&
           "Posting List iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override { return IndexCursor.count(); }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override {
    return OS << "[IndexIterator]";
  }

  mutable DatabaseIndexCursor IndexCursor;
  bool IsEnd = false;
};

} // namespace

// Mark symbols which are can be used for code completion.
const Token RestrictedForCodeCompletion =
    Token(Token::Kind::Sentinel, "Restricted For Code Completion");

std::vector<std::string> generateProximityURIs(llvm::StringRef URIPath) {
  std::vector<std::string> Result;
  auto ParsedURI = URI::parse(URIPath);
  assert(ParsedURI &&
         "Non-empty argument of generateProximityURIs() should be a valid "
         "URI.");
  llvm::StringRef Body = ParsedURI->body();
  // FIXME(kbobyrev): Currently, this is a heuristic which defines the maximum
  // size of resulting vector. Some projects might want to have higher limit
  // if the file hierarchy is deeper. For the generic case, it would be useful
  // to calculate Limit in the index build stage by calculating the maximum
  // depth of the project source tree at runtime.
  size_t Limit = 5;
  // Insert original URI before the loop: this would save a redundant
  // iteration with a URI parse.
  Result.emplace_back(ParsedURI->toString());
  while (!Body.empty() && --Limit > 0) {
    // FIXME(kbobyrev): Parsing and encoding path to URIs is not necessary and
    // could be optimized.
    Body = llvm::sys::path::parent_path(Body, llvm::sys::path::Style::posix);
    if (!Body.empty())
      Result.emplace_back(
          URI(ParsedURI->scheme(), ParsedURI->authority(), Body).toString());
  }
  return Result;
}

llvm::ErrorOr<LMDBInstance::QueryRequest>
LMDBInstance::QueryRequest::begin(LMDBInstance &DB) {
  QueryRequest Ret;
  Ret.DB = DB.getInstance();
  if (auto TransOrErr = LMDBInstance::TransactionImpl::begin(DB, true);
      !TransOrErr)
    return TransOrErr.getError();
  else
    Ret.Snapshot = std::move(*TransOrErr);
  auto Size = Ret.DB->IndexedSymbolsDB.numberOfEntries(Ret.Snapshot->getTxn());
  if (!Size)
    return Size.getError();
  Ret.Corpus = db_index::Corpus(*Size);
  return Ret;
}

std::unique_ptr<Iterator>
LMDBInstance::QueryRequest::iterator(const Token &Tok) const {
  auto HashedToken = Tok.getXxHash();
  auto It = DB->IndexedSymbolsDB.findOid(Snapshot->getTxn(),
                                         key_id::INDEXEDSYMBOL_TOKEN,
                                         typeToStringRef(HashedToken));
  if (!It)
    return Corpus.none();
  return std::make_unique<IndexIterator>(std::move(*It));
}

// Constructs BOOST iterators for Path Proximities.
std::unique_ptr<Iterator>
LMDBInstance::QueryRequest::createFileProximityIterator(
    llvm::ArrayRef<std::string> ProximityPaths) const {
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
    auto It = iterator(Token(Token::Kind::ProximityURI, ParentURI));
    if (It->kind() != Iterator::Kind::False) {
      PathProximitySignals.SymbolURI = ParentURI;
      BoostingIterators.push_back(Corpus.boost(
          std::move(It), PathProximitySignals.evaluateHeuristics()));
    }
  }
  BoostingIterators.push_back(Corpus.all());
  return Corpus.unionOf(std::move(BoostingIterators));
}

// Constructs BOOST iterators for preferred types.
std::unique_ptr<Iterator>
LMDBInstance::QueryRequest::createTypeBoostingIterator(
    llvm::ArrayRef<std::string> Types) const {
  std::vector<std::unique_ptr<Iterator>> BoostingIterators;
  SymbolRelevanceSignals PreferredTypeSignals;
  PreferredTypeSignals.TypeMatchesPreferred = true;
  auto Boost = PreferredTypeSignals.evaluateHeuristics();
  for (const auto &T : Types)
    BoostingIterators.push_back(
        Corpus.boost(iterator(Token(Token::Kind::Type, T)), Boost));
  BoostingIterators.push_back(Corpus.all());
  return Corpus.unionOf(std::move(BoostingIterators));
}

void LMDBInstance::QueryRequest::lookup(
    const LookupRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  for (const auto &ID : Req.IDs) {
    auto IndexedSym = Snapshot->findSymbol(ID);
    if (IndexedSym) {
      Callback(Snapshot->getMarshaller().toNative(IndexedSym->Symbol));
    }
  }
}

bool LMDBInstance::QueryRequest::refs(
    const RefsRequest &Req,
    llvm::function_ref<void(const Ref &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  for (const auto &ID : Req.IDs) {
    auto It = Snapshot->findReferences(ID);
    if (!It)
      continue;
    for (llvm::ErrorOr<OID> OidOrErr = It->getOID();
         OidOrErr && *OidOrErr != EmptyOid; OidOrErr = It->next()) {
      auto SR = DB->ShardRefsDB.readOid(Snapshot->getTxn(), *OidOrErr);
      if (!SR)
        continue;
      for (const auto &V : SR->Data.second) {
        if (Remaining == 0)
          return true;
        --Remaining;
        Callback(Snapshot->getMarshaller().toNative(V));
      }
    }
  }
  return false;
}

bool LMDBInstance::QueryRequest::fuzzyFind(
    const FuzzyFindRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) {
  assert(!StringRef(Req.Query).contains("::") &&
         "There must be no :: in query.");
  FuzzyMatcher Filter(Req.Query);
  // For short queries we use specialized trigrams that don't yield all
  // results. Prevent clients from postfiltering them for longer queries.
  bool More = !Req.Query.empty() && Req.Query.size() < 3;

  std::vector<std::unique_ptr<Iterator>> Criteria;
  const auto TrigramTokens = generateQueryTrigrams(Req.Query);

  // Generate query trigrams and construct AND iterator over all query
  // trigrams.
  std::vector<std::unique_ptr<Iterator>> TrigramIterators;
  for (const auto &Trigram : TrigramTokens)
    TrigramIterators.push_back(iterator(Trigram));
  Criteria.push_back(Corpus.intersect(std::move(TrigramIterators)));

  // Generate scope tokens for search query.
  std::vector<std::unique_ptr<Iterator>> ScopeIterators;
  for (const auto &Scope : Req.Scopes)
    ScopeIterators.push_back(iterator(Token(Token::Kind::Scope, Scope)));
  if (Req.AnyScope)
    ScopeIterators.push_back(
        Corpus.boost(Corpus.all(), ScopeIterators.empty() ? 1.0 : 0.2));
  Criteria.push_back(Corpus.unionOf(std::move(ScopeIterators)));

  // Add proximity paths boosting (all symbols, some boosted).
  Criteria.push_back(createFileProximityIterator(Req.ProximityPaths));
  // Add boosting for preferred types.
  Criteria.push_back(createTypeBoostingIterator(Req.PreferredTypes));

  if (Req.RestrictForCodeCompletion)
    Criteria.push_back(iterator(RestrictedForCodeCompletion));

  // Use TRUE iterator if both trigrams and scopes from the query are not
  // present in the symbol index.
  auto Root = Corpus.intersect(std::move(Criteria));
  // Retrieve more items than it was requested: some of  the items with high
  // final score might not be retrieved otherwise.
  // FIXME(kbobyrev): Tune this ratio.
  if (Req.Limit)
    Root = Corpus.limit(std::move(Root), *Req.Limit * 100);

  using IDAndScore = std::pair<db_index::DocID, float>;
  std::vector<IDAndScore> IDAndScores = consume(*Root);
  llvm::DenseMap<DocID, std::pair<cl::Symbol, Symbol>> Symbols;

  auto Compare = [](const IDAndScore &LHS, const IDAndScore &RHS) {
    return LHS.second > RHS.second;
  };
  TopN<IDAndScore, decltype(Compare)> Top(
      Req.Limit ? *Req.Limit : std::numeric_limits<size_t>::max(), Compare);
  for (const auto &IDAndScore : IDAndScores) {
    const db_index::DocID SymbolDocID = IDAndScore.first;
    auto IndexedSymOrErr =
        DB->IndexedSymbolsDB.readOid(Snapshot->getTxn(), SymbolDocID);
    if (!IndexedSymOrErr)
      DB->raiseFatalError(IndexDBError::Corrupted);
    const std::optional<float> Score =
        Filter.match(llvm::StringRef(IndexedSymOrErr->Symbol.Name));
    if (!Score)
      continue;
    auto &[ClSym, Sym] = Symbols[SymbolDocID];
    ClSym = IndexedSymOrErr->Symbol;
    Sym = Snapshot->getMarshaller().toNative(ClSym);
    // Combine Fuzzy Matching score, precomputed symbol quality and boosting
    // score for a cumulative final symbol score.
    const float FinalScore = (*Score) * quality(Sym) * IDAndScore.second;
    // If Top.push(...) returns true, it means that it had to pop an item. In
    // this case, it is possible to retrieve more symbols.
    if (Top.push({SymbolDocID, FinalScore}))
      More = true;
  }

  // Apply callback to the top Req.Limit items in the descending
  // order of cumulative score.
  for (const auto &Item : std::move(Top).items())
    Callback(Symbols[Item.first].second);
  return More;
}

void LMDBInstance::QueryRequest::relations(
    const RelationsRequest &Req,
    llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  for (const auto &ID : Req.Subjects) {
    auto It = Snapshot->findRelations(ID, Req.Predicate);
    if (!It)
      continue;
    for (llvm::ErrorOr<OID> OidOrErr = It->getOID();
         OidOrErr && *OidOrErr != EmptyOid; OidOrErr = It->next()) {
      auto Rel = DB->ShardRelationsDB.readOid(Snapshot->getTxn(), *OidOrErr);
      if (Remaining == 0)
        return;
      --Remaining;
      auto Sym = Snapshot->findSymbol(
          Snapshot->getMarshaller().toNative(Rel->Data.Object));
      if (Sym)
        Callback(ID, Snapshot->getMarshaller().toNative(Sym->Symbol));
    }
  }
}

LMDBInstance::LMDBInstance(llvm::StringRef Path) {
  auto EnvOrErr = lmdb::Environment::create();
  if (!EnvOrErr)
    return;
  Env = std::move(*EnvOrErr);

  Env.setMaxDBs([this]() {
    MDB_dbi Count = 0;
    for (auto &I : Databases)
      Count += I->databaseCount();
    return Count;
  }());
  Env.open(Path.data(),
           MDB_NOTLS | MDB_NOMETASYNC | MDB_WRITEMAP | MDB_WRITEMAP_FSYNC);

  auto TxOrErr = lmdb::Txn::begin(Env);
  if (!TxOrErr)
    return;
  auto Tx = std::move(*TxOrErr);
  for (auto &I : Databases) {
    if (auto EC = I->open(Tx))
      return;
  }
  refreshEnvMapSize();
  Tx.commit();
}

LMDBInstance::~LMDBInstance() {
  ::MDB_envinfo EnvInfo = Env.getEnvInfo();
  log("~LMDBInstance(): mapsize = {0}, last_pgno = {1}, last_txnid = {2}",
      EnvInfo.me_mapsize, EnvInfo.me_last_pgno, EnvInfo.me_last_txnid);
}

std::shared_ptr<LMDBInstance>
LMDBInstance::createInstance(llvm::StringRef Path) {
  return std::shared_ptr<LMDBInstance>(new LMDBInstance(Path));
}

void LMDBInstance::raiseFatalError(std::error_code EC) {
  llvm::report_fatal_error(
      llvm::Twine("Database fatal error! Error message: " + EC.message()));
}

bool LMDBInstance::refreshEnvMapSize() {
  ::MDB_envinfo EnvInfo = Env.getEnvInfo();
  if (EnvMapSize == EnvInfo.me_mapsize)
    return false;
  EnvMapSize = EnvInfo.me_mapsize;
  return true;
}

llvm::ErrorOr<std::pair<OID, cl::IndexFileInfo>>
LMDBInstance::TransactionImpl::findIndexFileInfo(llvm::StringRef ShardPath) {
  auto ItOrErr = DB->ShardDB.findOid(Txn, key_id::SHARD_SHARDPATH,
                                     typeToStringRef(digest(ShardPath)));
  if (!ItOrErr)
    return ItOrErr.getError();
  auto &It = *ItOrErr;
  std::error_code Err;
  for (llvm::ErrorOr<OID> OidOrErr = It.getOID();
       OidOrErr && *OidOrErr != EmptyOid;
       OidOrErr = It.next(), Err = OidOrErr.getError()) {
    auto Shard = DB->ShardDB.readOid(Txn, *OidOrErr);
    if (Shard && Shard->ShardPath == ShardPath)
      return std::pair{*OidOrErr, *Shard};
  }
  return Err ? Err : IndexDBError::Notfound;
}

std::unique_ptr<IndexFileInfo>
LMDBInstance::TransactionImpl::getShardInfo(llvm::StringRef ShardPath) {
  auto ShardOrErr = findIndexFileInfo(ShardPath);
  if (!ShardOrErr || !ShardOrErr->second.Valid)
    return nullptr;
  IndexFileInfo IF = Marshaller.fromIndexFileInfo(&ShardOrErr->second);
  return std::make_unique<IndexFileInfo>(std::move(IF));
}

llvm::ErrorOr<cl::Symbol>
LMDBInstance::TransactionImpl::lookupShardsSymbol(SymbolID ID) {
  auto ItOrErr = DB->ShardSymbolsDB.findOid(
      Txn, key_id::SHARD_SYMBOL_ID, typeToStringRef(Marshaller.fromNative(ID)));
  if (!ItOrErr)
    return ItOrErr.getError();
  llvm::SmallVector<cl::Symbol> SymStorage;
  auto &It = *ItOrErr;
  std::error_code Err;
  for (llvm::ErrorOr<OID> OidOrErr = It.getOID();
       OidOrErr && *OidOrErr != EmptyOid;
       OidOrErr = It.next(), Err = OidOrErr.getError()) {
    auto ShardSym = DB->ShardSymbolsDB.readOid(Txn, *OidOrErr);
    if (!ShardSym)
      return IndexDBError::Corrupted;
    SymStorage.emplace_back(std::move(ShardSym->Data));
  }
  if (Err)
    return Err;
  assert(!SymStorage.empty());
  cl::Symbol Sym;
  for (size_t I = 0; I < SymStorage.size(); ++I) {
    Sym = (I == 0) ? SymStorage[I] : mergeSymbol(Sym, SymStorage[I]);
  }
  return Sym;
}

std::optional<cl::IndexedSymbol>
LMDBInstance::TransactionImpl::findSymbol(SymbolID ID) {
  auto It =
      DB->IndexedSymbolsDB.findOid(Txn, key_id::INDEXEDSYMBOL_ID, ID.raw());
  if (!It)
    return std::nullopt;
  auto IndexedSym = DB->IndexedSymbolsDB.readOid(Txn, It->getOID());
  if (!IndexedSym)
    DB->raiseFatalError(IndexDBError::Corrupted);
  return *IndexedSym;
}

llvm::ErrorOr<DatabaseIndexCursor>
LMDBInstance::TransactionImpl::findReferences(SymbolID ID) {
  return DB->ShardRefsDB.findOid(Txn, key_id::SHARD_REFS_ID, ID.raw());
}

llvm::ErrorOr<DatabaseIndexCursor>
LMDBInstance::TransactionImpl::findRelations(SymbolID ID,
                                             RelationKind Predicate) {
  std::pair Key{Marshaller.fromNative(ID), Predicate};
  return DB->ShardRelationsDB.findOid(Txn, key_id::SHARD_RELATION_KEY,
                                      typeToStringRef(Key));
}

llvm::ErrorOr<OID> LMDBInstance::TransactionImpl::createIndexFileInfo(
    llvm::StringRef ShardPath, const cl::IndexFileInfo *ShardInfo) {
  if (ShardInfo == nullptr)
    return DB->ShardDB.allocOid(Txn,
                                Marshaller.toEmptyIndexFileInfo(ShardPath));
  return DB->ShardDB.allocOid(Txn, *ShardInfo);
}

std::error_code LMDBInstance::TransactionImpl::updateSymbol(SymbolID ID) {
  std::optional<OID> SymOid;
  auto ItOrErr =
      DB->IndexedSymbolsDB.findOid(Txn, key_id::INDEXEDSYMBOL_ID, ID.raw());
  if (!ItOrErr && ItOrErr.getError() != IndexDBError::Notfound)
    return ItOrErr.getError();
  if (ItOrErr)
    SymOid = ItOrErr->getOID();
  auto SymOrErr = lookupShardsSymbol(ID);
  auto EC = SymOrErr.getError();
  if (SymOrErr) {
    auto &Sym = *SymOrErr;
    auto Apply = [&](const Token &A) { return A.getXxHash(); };
    auto Tokens = buildTokens(Sym);
    cl::IndexedSymbol IndexedSym{Sym,
                                 {llvm::map_iterator(Tokens.begin(), Apply),
                                  llvm::map_iterator(Tokens.end(), Apply)}};
    if (!SymOid) {
      auto OidOrErr = DB->IndexedSymbolsDB.allocOid(Txn, {});
      if (!OidOrErr)
        return OidOrErr.getError();
      SymOid = *OidOrErr;
    }
    EC = DB->IndexedSymbolsDB.writeOid(Txn, *SymOid, IndexedSym);
  } else if (EC == IndexDBError::Notfound && SymOid) {
    EC = DB->IndexedSymbolsDB.removeOid(Txn, *SymOid);
  }
  return EC;
}

std::error_code
LMDBInstance::TransactionImpl::updateShard(llvm::StringRef ShardPath,
                                           const IndexFileOut *Shard,
                                           int64_t ModifiedTime) {
  bool RemoveShard = Shard == nullptr;
  auto RemoveShardObjects =
      [&](auto &Database, OID ShardOid, IndexID IndexId,
          llvm::function_ref<void(const typename std::remove_reference_t<
                                  decltype(Database)>::Type &)>
              Action = nullptr) -> std::error_code {
    auto EC = Database.removeOidsByKey(Txn, IndexId, typeToStringRef(ShardOid),
                                       Action);
    if (EC && EC != IndexDBError::Notfound)
      return EC;
    return std::error_code();
  };

  llvm::DenseSet<SymbolID> UpdatedSyms;
  auto PrevShard = findIndexFileInfo(ShardPath);
  if (PrevShard) {
    // Old shard's data needs to be removed first.
    OID ShardOid = PrevShard->first;
    std::error_code EC = RemoveShardObjects(
        DB->ShardSymbolsDB, ShardOid, key_id::SHARD_SYMBOL_PROVIDER,
        [&](const cl::ShardSymbolRow &Sym) {
          SymbolID ID = Marshaller.toNative(Sym.Data.ID);
          UpdatedSyms.insert(ID);
        });
    if (EC)
      return EC;
    EC = RemoveShardObjects(DB->ShardRefsDB, ShardOid,
                            key_id::SHARD_REFS_PROVIDER);
    if (EC)
      return EC;
    EC = RemoveShardObjects(DB->ShardRelationsDB, ShardOid,
                            key_id::SHARD_RELATION_PROVIDER);
    if (EC)
      return EC;

    if (RemoveShard) {
      EC = DB->ShardDB.removeOid(Txn, ShardOid);
      if (EC)
        DB->raiseFatalError(IndexDBError::Corrupted);
    }
  } else {
    std::error_code EC = PrevShard.getError();
    if (EC != IndexDBError::Notfound)
      return EC;
  }

  if (!RemoveShard) {
    auto ShardInfo =
        Marshaller.toIndexFileInfo(ShardPath, *Shard, ModifiedTime);
    llvm::ErrorOr<OID> ShardOidOrErr = [&]() -> llvm::ErrorOr<OID> {
      if (!PrevShard)
        return createIndexFileInfo(ShardPath, &ShardInfo);
      if (auto EC = DB->ShardDB.writeOid(Txn, PrevShard->first, ShardInfo))
        return EC;
      return PrevShard->first;
    }();
    if (!ShardOidOrErr)
      return ShardOidOrErr.getError();
    OID ShardOid = *ShardOidOrErr;

    for (const auto &V : *Shard->Symbols) {
      auto OidOrErr = DB->ShardSymbolsDB.allocOid(
          Txn, cl::ShardSymbolRow{ShardOid, Marshaller.fromNative(V)});
      if (!OidOrErr)
        return OidOrErr.getError();
      UpdatedSyms.insert(V.ID);
    }
    for (const auto &V : *Shard->Refs) {
      auto Apply = [&](const Ref &V) { return Marshaller.fromNative(V); };
      auto OidOrErr = DB->ShardRefsDB.allocOid(
          Txn, {ShardOid,
                {Marshaller.fromNative(V.first),
                 {llvm::map_iterator(V.second.begin(), Apply),
                  llvm::map_iterator(V.second.end(), Apply)}}});
      if (!OidOrErr)
        return OidOrErr.getError();
    }
    for (const auto &V : *Shard->Relations) {
      auto OidOrErr = DB->ShardRelationsDB.allocOid(
          Txn, cl::ShardRelationRow{ShardOid, Marshaller.fromNative(V)});
      if (!OidOrErr)
        return OidOrErr.getError();
    }
  }

  for (const auto &V : UpdatedSyms) {
    if (auto EC = updateSymbol(V))
      return EC;
  }

  return std::error_code();
}

llvm::ErrorOr<std::unique_ptr<LMDBInstance::TransactionImpl>>
LMDBInstance::TransactionImpl::begin(LMDBInstance &DB, bool RO) {
  decltype(TransactionImpl::TxnLk) TxnLk;
  if (RO)
    TxnLk = std::shared_lock<std::shared_mutex>(DB.TransactionMu);
  else
    TxnLk = std::unique_lock<std::shared_mutex>(DB.TransactionMu);
  llvm::ErrorOr<lmdb::Txn> TxOrErr = std::error_code();
  do {
    TxOrErr = lmdb::Txn::begin(DB.Env, RO ? MDB_RDONLY : 0);
    if (!TxOrErr) {
      auto EC = TxOrErr.getError();
      if (EC != lmdb::DBError::MapResized)
        return EC;
      // Refresh the map size as it was changed outside our process.
      DB.Env.setMapSize(0);
      DB.refreshEnvMapSize();
    }
  } while (!TxOrErr);
  return std::unique_ptr<TransactionImpl>(
      new TransactionImpl(DB, std::move(TxnLk), std::move(*TxOrErr)));
}

size_t LMDBInstance::databaseSize() { return EnvMapSize; }

std::error_code
LMDBInstance::growDatabaseSize(std::optional<size_t> OldSizeHint) {
  constexpr mdb_size_t SizeCap =
      mdb_size_t(2) * 1024 * 1024 * 1024 * 1024; // 2TB for now.
  std::unique_lock<std::shared_mutex> Lk(TransactionMu);
  if (refreshEnvMapSize() || (OldSizeHint && OldSizeHint < EnvMapSize))
    return std::error_code();
  if (auto EC = Env.setMapSize(std::min(EnvMapSize << 1, SizeCap)))
    return EC;
  refreshEnvMapSize();
  return std::error_code();
}

llvm::ErrorOr<std::unique_ptr<Transaction>>
LMDBInstance::beginTransaction(bool RO) {
  auto TransOrErr = TransactionImpl::begin(*this, RO);
  if (!TransOrErr)
    return TransOrErr.getError();
  return std::move(*TransOrErr);
}

std::unique_ptr<IndexFileInfo>
LMDBInstance::getShardInfo(Transaction &TransIn, llvm::StringRef ShardPath) {
  TransactionImpl &Trans = static_cast<TransactionImpl &>(TransIn);
  return Trans.getShardInfo(ShardPath);
}

std::error_code LMDBInstance::updateShard(Transaction &TransIn,
                                          llvm::StringRef ShardPath,
                                          const IndexFileOut *Shard,
                                          int64_t ModifiedTime) {
  TransactionImpl &Trans = static_cast<TransactionImpl &>(TransIn);
  return Trans.updateShard(ShardPath, Shard, ModifiedTime);
}

std::shared_ptr<LMDB> createSharedDatabase(llvm::StringRef ProjectPath) {
  llvm::SmallString<16> DatabasePath = ProjectPath;
  if (DatabasePath.empty()) {
    if (!llvm::sys::path::cache_directory(DatabasePath))
      return nullptr;
  } else {
    llvm::sys::path::append(DatabasePath, ".cache");
  }
  llvm::sys::path::append(DatabasePath, "clangd", "db-index");
  if (llvm::sys::fs::create_directories(DatabasePath))
    return nullptr;
  return LMDBInstance::createInstance(DatabasePath);
}

std::unique_ptr<SymbolIndex> createSymbolIndex(std::shared_ptr<LMDB> DB) {
  auto Inst = std::static_pointer_cast<LMDBInstance>(DB);
  return std::make_unique<LMDBIndexImpl>(Inst);
}

} // namespace db_index
} // namespace clangd
} // namespace clang
