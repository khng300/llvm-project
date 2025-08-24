//===--- DocumentStoreQuery.cpp - Symbol database ---------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "DocumentStoreQuery.h"
#include "FuzzyMatch.h"
#include "Quality.h"
#include "SymbolIndex.h"
#include "Trigram.h"
#include "support/Logger.h"

#include <optional>
#include <system_error>

namespace clang {
namespace clangd {
namespace lmdb_index {

namespace {

/// Implements iterator of PostingList chunks. This requires iterating over two
/// levels: the first level iterator iterates over the chunks and decompresses
/// them on-the-fly when the contents of chunk are to be seen.
class IndexIterator : public Iterator {
public:
  explicit IndexIterator(IndexOIDIterator &&It) : IterImpl(std::move(It)) {}

  bool reachedEnd() const override { return IsEnd; }

  /// Advances cursor to the next item.
  void advance() override {
    assert(!reachedEnd() &&
           "Posting List iterator can't advance() at the end.");
    auto Oid = IterImpl.next();
    IsEnd = !Oid;
  }

  /// Applies binary search to advance cursor to the next item with DocID
  /// equal or higher than the given one.
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() &&
           "Posting List iterator can't advance() at the end.");
    if (ID <= peek())
      return;
    auto Oid = IterImpl.gotoOid(ID);
    IsEnd = !Oid;
  }

  DocID peek() const override {
    assert(!reachedEnd() && "Posting List iterator can't peek() at the end.");
    auto Oid = IterImpl.peek();
    assert(Oid);
    return *Oid;
  }

  float consume() override {
    assert(!reachedEnd() &&
           "Posting List iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override { return IterImpl.count(); }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override {
    return OS << "[IndexIterator]";
  }

  mutable IndexOIDIterator IterImpl;
  bool IsEnd = false;
};

class SymbolsIterator : public Iterator {
public:
  explicit SymbolsIterator(OIDIterator &&It) : IterImpl(std::move(It)) {}

  bool reachedEnd() const override { return IsEnd; }

  /// Advances cursor to the next item.
  void advance() override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't advance() at the end.");
    auto Oid = IterImpl.next();
    IsEnd = !Oid;
  }

  /// Applies binary search to advance cursor to the next item with DocID
  /// equal or higher than the given one.
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't advance() at the end.");
    if (ID <= peek())
      return;
    auto Oid = IterImpl.gotoOid(ID);
    IsEnd = !Oid;
  }

  DocID peek() const override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't peek() at the end.");
    auto Oid = IterImpl.peek();
    assert(Oid);
    return *Oid;
  }

  float consume() override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override { return IterImpl.count(); }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override {
    return OS << "[SymbolsIterator]";
  }

  mutable OIDIterator IterImpl;
  bool IsEnd = false;
};

} // namespace

llvm::ErrorOr<DocumentStoreQuery>
DocumentStoreQuery::begin(DocumentStoreImpl &DB) {
  DocumentStoreQuery Ret;
  Ret.DB = DB.getInstance();
  auto Snapshot = DocumentStoreImpl::TransactionImpl::begin(DB, true, {});
  if (!Snapshot)
    return Snapshot.getError();
  Ret.Snapshot = std::move(*Snapshot);
  auto LastOid = Ret.DB->IndexedSymbolsDB.lastOid(Ret.Snapshot->getTxn());
  if (!LastOid)
    return LastOid.getError();
  if (LastOid->has_value())
    Ret.Corpus = lmdb_index::Corpus(**LastOid + 1);
  return Ret;
}

std::unique_ptr<Iterator> DocumentStoreQuery::iterator(const Token &Tok) const {
  auto HashedToken = Tok.getXxHash();
  auto ItOrErr = DB->IndexedSymbolsDB.findOid(
      Snapshot->getTxn(), key_id::INDEXEDSYMBOL_TOKEN, makeSlice(HashedToken));
  if (!ItOrErr)
    return Corpus.none();
  return std::make_unique<IndexIterator>(std::move(*ItOrErr));
}

// Constructs BOOST iterators for Path Proximities.
std::unique_ptr<Iterator> DocumentStoreQuery::createFileProximityIterator(
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
std::unique_ptr<Iterator> DocumentStoreQuery::createTypeBoostingIterator(
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

void DocumentStoreQuery::lookup(
    const LookupRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  for (const auto &ID : Req.IDs) {
    auto IndexedSym = Snapshot->findSymbol(ID);
    if (IndexedSym) {
      auto Res = Snapshot->getMarshaller().toNative(IndexedSym->Symbol);
      if (!Res)
        DB->raiseFatalError(Res.getError());
      Callback(*Res);
    }
  }
}

bool DocumentStoreQuery::refs(
    const RefsRequest &Req,
    llvm::function_ref<void(const Ref &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  auto &Marshaller = Snapshot->getMarshaller();
  for (const auto &ID : Req.IDs) {
    cl::SymbolID SymId = Marshaller.fromNative(ID);
    auto CurOrErr = Snapshot->getKeyToFileDbiCursor(DB->RefSymbolIDToIDFileDbi,
                                                    makeSlice(SymId));
    if (!CurOrErr) {
      auto EC = CurOrErr.getError();
      if (EC != IndexDBError::Notfound)
        DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
      continue;
    }

    auto It = lmdb::DupIterator(std::move(*CurOrErr));
    std::optional<Slice> Data;
    while ((Data = It.next())) {
      auto Oid = Data->toTypeRef<OID>();
      auto IDFileOrErr = Snapshot->getIndexDataFile(Oid);
      if (!IDFileOrErr)
        DB->raiseFatalError(IndexDBError::DBInconsistent);
      const cl::IndexDataFile &IDFile = **IDFileOrErr;
      auto RefListIt = IDFile.Refs.find(SymId);
      if (RefListIt == IDFile.Refs.end())
        DB->raiseFatalError(IndexDBError::DBInconsistent);
      for (const auto &V : RefListIt->second) {
        if (Remaining == 0)
          return true;
        --Remaining;
        auto Ref = Snapshot->getMarshaller().toNative(V);
        if (!Ref)
          DB->raiseFatalError(Ref.getError());
        Callback(*Ref);
      }
    }
    if (auto EC = It.errorCode())
      DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
  }
  return false;
}

bool DocumentStoreQuery::containedRefs(
    const ContainedRefsRequest &Req,
    llvm::function_ref<void(const ContainedRefsResult &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  auto &Marshaller = Snapshot->getMarshaller();
  cl::SymbolID SymId = Marshaller.fromNative(Req.ID);
  auto CurOrErr = Snapshot->getKeyToFileDbiCursor(DB->RevRefSymbolIDToIDFileDbi,
                                                  makeSlice(SymId));
  if (!CurOrErr) {
    auto EC = CurOrErr.getError();
    if (EC != IndexDBError::Notfound)
      DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
    return false;
  }

  auto It = lmdb::DupIterator(std::move(*CurOrErr));
  std::optional<Slice> Data;
  while ((Data = It.next())) {
    auto Oid = Data->toTypeRef<OID>();
    auto IDFileOrErr = Snapshot->getIndexDataFile(Oid);
    if (!IDFileOrErr)
      DB->raiseFatalError(IndexDBError::DBInconsistent);
    const cl::IndexDataFile &IDFile = **IDFileOrErr;
    auto RevRefListIt = IDFile.RevRefs.find(SymId);
    if (RevRefListIt == IDFile.RevRefs.end())
      DB->raiseFatalError(IndexDBError::DBInconsistent);
    for (const auto &V : RevRefListIt->second) {
      if (Remaining == 0)
        return true;
      --Remaining;
      auto RevRef = Snapshot->getMarshaller().toNative(V);
      if (!RevRef)
        DB->raiseFatalError(RevRef.getError());
      Callback(*RevRef);
    }
  }
  if (auto EC = It.errorCode())
    DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
  return false;
}

bool DocumentStoreQuery::fuzzyFind(
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

  {
    auto Res = DB->IndexedSymbolsDB.getOidCursor(Snapshot->getTxn());
    if (!Res)
      DB->raiseFatalError(Res.getError());
    Criteria.push_back(std::make_unique<SymbolsIterator>(std::move(*Res)));
  }

  // Use TRUE iterator if both trigrams and scopes from the query are not
  // present in the symbol index.
  auto Root = Corpus.intersect(std::move(Criteria));
  // Retrieve more items than it was requested: some of  the items with high
  // final score might not be retrieved otherwise.
  // FIXME(kbobyrev): Tune this ratio.
  if (Req.Limit)
    Root = Corpus.limit(std::move(Root), *Req.Limit * 100);

  using IDAndScore = std::pair<lmdb_index::DocID, float>;
  std::vector<IDAndScore> IDAndScores = consume(*Root);
  std::unordered_map<DocID, std::pair<cl::Symbol, Symbol>> Symbols;

  auto Compare = [](const IDAndScore &LHS, const IDAndScore &RHS) {
    return LHS.second > RHS.second;
  };
  TopN<IDAndScore, decltype(Compare)> Top(
      Req.Limit ? *Req.Limit : std::numeric_limits<size_t>::max(), Compare);
  for (const auto &IDAndScore : IDAndScores) {
    const lmdb_index::DocID SymbolDocID = IDAndScore.first;
    auto IndexedSym =
        DB->IndexedSymbolsDB.readOid(Snapshot->getTxn(), SymbolDocID);
    if (!IndexedSym)
      DB->raiseFatalError(IndexDBError::Corrupted);
    const std::optional<float> Score =
        Filter.match(llvm::StringRef(IndexedSym->Symbol.Name));
    if (!Score)
      continue;
    auto &[SerializedSym, Sym] = Symbols[SymbolDocID];
    SerializedSym = IndexedSym->Symbol;
    auto Res = Snapshot->getMarshaller().toNative(SerializedSym);
    if (!Res)
      DB->raiseFatalError(Res.getError());
    Sym = *Res;
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

void DocumentStoreQuery::relations(
    const RelationsRequest &Req,
    llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  auto &Marshaller = Snapshot->getMarshaller();
  for (const auto &ID : Req.Subjects) {
    auto PredicateKey =
        cl::data::pair{Marshaller.fromNative(ID), Req.Predicate};
    auto CurOrErr = Snapshot->getKeyToFileDbiCursor(DB->RelationKeyToIDFileDbi,
                                                    makeSlice(PredicateKey));
    if (!CurOrErr) {
      auto EC = CurOrErr.getError();
      if (EC != IndexDBError::Notfound)
        DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
      continue;
    }

    auto It = lmdb::DupIterator(std::move(*CurOrErr));
    std::optional<Slice> Data;
    while ((Data = It.next())) {
      auto Oid = Data->toTypeRef<OID>();
      auto IDFileOrErr = Snapshot->getIndexDataFile(Oid);
      if (!IDFileOrErr)
        DB->raiseFatalError(IndexDBError::DBInconsistent);
      const cl::IndexDataFile &IDFile = **IDFileOrErr;
      auto RelIt = IDFile.Relations.find(PredicateKey);
      if (RelIt == IDFile.Relations.end())
        DB->raiseFatalError(IndexDBError::DBInconsistent);
      for (const auto &Rel : RelIt->second) {
        if (Remaining == 0)
          return;
        --Remaining;
        auto SymOrErr = Snapshot->findSymbol(
            Snapshot->getMarshaller().toNative(Rel.Object));
        if (!SymOrErr)
          DB->raiseFatalError(SymOrErr.getError());
        auto Sym = Snapshot->getMarshaller().toNative(SymOrErr->Symbol);
        if (!Sym)
          DB->raiseFatalError(Sym.getError());
        Callback(ID, *Sym);
      }
    }
    if (auto EC = It.errorCode())
      DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
  }
}

std::unique_ptr<SymbolIndex>
createSymbolIndex(std::shared_ptr<DocumentStore> DB) {
  auto Inst = std::static_pointer_cast<DocumentStoreImpl>(DB);
  return std::make_unique<LMDBIndexImpl>(Inst);
}

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
