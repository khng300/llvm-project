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

#include <optional>
#include <system_error>

using namespace clang::clangd::lmdb_index::internal;

namespace clang {
namespace clangd {
namespace lmdb_index {

namespace {

/// Implements iterator of PostingList chunks. This requires iterating over two
/// levels: the first level iterator iterates over the chunks and decompresses
/// them on-the-fly when the contents of chunk are to be seen.
class IndexIterator : public Iterator {
public:
  explicit IndexIterator(lmdb::DupIterator &&It) : IterImpl(std::move(It)) {
    auto OidSlice = IterImpl.peek();
    if (!OidSlice) {
      IsEnd = true;
      return;
    }
    Oid = OidSlice->toTypeRef<OID>();
  }

  bool reachedEnd() const override { return IsEnd; }

  /// Advances cursor to the next item.
  void advance() override {
    assert(!reachedEnd() &&
           "Posting List iterator can't advance() at the end.");
    auto OidSlice = IterImpl.next();
    if (OidSlice)
      Oid = OidSlice->toTypeRef<OID>();
    IsEnd = !OidSlice;
  }

  /// Applies binary search to advance cursor to the next item with DocID
  /// equal or higher than the given one.
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() &&
           "Posting List iterator can't advance() at the end.");
    if (ID <= peek())
      return;
    auto OidSlice = IterImpl.seek(makeSlice(ID));
    if (OidSlice)
      Oid = OidSlice->toTypeRef<OID>();
    IsEnd = !OidSlice;
  }

  DocID peek() const override {
    assert(!reachedEnd() && "Posting List iterator can't peek() at the end.");
    return Oid;
  }

  float consume() override {
    assert(!reachedEnd() &&
           "Posting List iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override {
    auto Count = IterImpl.count();
    if (!Count)
      return 0;
    return *Count;
  }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override {
    return OS << "[IndexIterator]";
  }

  mutable lmdb::DupIterator IterImpl;
  OID Oid;
  bool IsEnd = false;
};

class SymbolsIterator : public Iterator {
public:
  explicit SymbolsIterator(lmdb::Cursor &&Cur) : Cursor(std::move(Cur)) {
    Slice Key;
    auto EC = Cursor.get(Key, MDB_GET_CURRENT);
    if (EC) {
      IsEnd = true;
      return;
    }
    Oid = Key.toTypeRef<OID>();
  }

  bool reachedEnd() const override { return IsEnd; }

  /// Advances cursor to the next item.
  void advance() override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't advance() at the end.");
    Slice Key;
    auto EC = Cursor.get(Key, MDB_NEXT);
    if (EC) {
      IsEnd = true;
      return;
    }
    Oid = Key.toTypeRef<OID>();
  }

  /// Applies binary search to advance cursor to the next item with DocID
  /// equal or higher than the given one.
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't advance() at the end.");
    if (ID <= peek())
      return;
    Slice Key = makeSlice(ID);
    auto EC = Cursor.get(Key, MDB_SET_RANGE);
    if (EC) {
      IsEnd = true;
      return;
    }
    Oid = Key.toTypeRef<OID>();
  }

  DocID peek() const override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't peek() at the end.");
    return Oid;
  }

  float consume() override {
    assert(!reachedEnd() &&
           "Symbols database iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override {
    auto Count = Cursor.count();
    if (!Count)
      return 0;
    return *Count;
  }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override {
    return OS << "[SymbolsIterator]";
  }

  mutable lmdb::Cursor Cursor;
  OID Oid;
  bool IsEnd = false;
};

} // namespace

llvm::ErrorOr<DocumentStoreQuery>
DocumentStoreQuery::begin(DocumentStoreImpl &DB) {
  DocumentStoreQuery Ret;
  Ret.DB = DB.getInstance();
  auto Snapshot = TransactionImpl::begin(DB, true, {});
  if (!Snapshot)
    return Snapshot.getError();
  Ret.Snapshot = std::move(*Snapshot);
  auto &Txn = Ret.Snapshot->getTxn();
  auto Stat = Ret.DB->IndexedSymbolsDbi.stat(Txn);
  if (!Stat)
    return Stat.getError();
  if (Stat->ms_entries != 0)
    Ret.Corpus = lmdb_index::Corpus(INT64_MAX);
  return Ret;
}

std::unique_ptr<Iterator> DocumentStoreQuery::iterator(const Token &Tok) const {
  auto HashedToken = Tok.getXxh3();
  auto CurOrErr =
      lmdb::Cursor::open(Snapshot->getTxn(), DB->HashedTokenToISymbolDbi);
  if (!CurOrErr)
    return Corpus.none();
  Slice Key = makeSlice(HashedToken);
  auto EC = CurOrErr->get(Key, MDB_SET);
  if (EC)
    return Corpus.none();
  return std::make_unique<IndexIterator>(std::move(*CurOrErr));
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

std::error_code DocumentStoreQuery::lookup(
    const LookupRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  for (const auto &ID : Req.IDs) {
    auto IndexedSym = Snapshot->findSymbol(ID);
    if (IndexedSym) {
      auto Res = Snapshot->getMarshaller().toNative(IndexedSym->Sym);
      if (!Res)
        return Res.getError();
      Callback(*Res);
    }
  }
  return {};
}

llvm::ErrorOr<bool>
DocumentStoreQuery::refs(const RefsRequest &Req,
                         llvm::function_ref<void(const Ref &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  auto &Marshaller = Snapshot->getMarshaller();
  for (const auto &ID : Req.IDs) {
    cl::SymbolID SymId = Marshaller.fromNative(ID);
    auto It = Snapshot->getKeyToFileDbiIterator(DB->RefSymbolIDToIDFileDbi,
                                                makeSlice(SymId));
    if (!It) {
      auto EC = It.getError();
      if (EC != DSError::Notfound)
        return EC;
      continue;
    }

    for (auto Res = It->peek(); Res; Res = It->next()) {
      const auto &IDFile = *Res->second;
      auto RefListIt = IDFile.Refs.find(SymId);
      if (RefListIt == IDFile.Refs.end())
        return DSError::DBInconsistent;
      for (const auto &V : RefListIt->second) {
        if (Remaining == 0)
          return true;
        --Remaining;
        auto Ref = Snapshot->getMarshaller().toNative(V);
        if (!Ref)
          return Ref.getError();
        Callback(*Ref);
      }
    }
    if (auto EC = It->errorCode())
      return EC;
  }
  return false;
}

llvm::ErrorOr<bool> DocumentStoreQuery::containedRefs(
    const ContainedRefsRequest &Req,
    llvm::function_ref<void(const ContainedRefsResult &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  auto &Marshaller = Snapshot->getMarshaller();
  cl::SymbolID SymId = Marshaller.fromNative(Req.ID);
  auto It = Snapshot->getKeyToFileDbiIterator(DB->RevRefSymbolIDToIDFileDbi,
                                              makeSlice(SymId));
  if (!It) {
    auto EC = It.getError();
    if (EC != DSError::Notfound)
      return EC;
    return false;
  }

  for (auto Res = It->peek(); Res; Res = It->next()) {
    const auto &IDFile = *Res->second;
    auto RevRefListIt = IDFile.RevRefs.find(SymId);
    if (RevRefListIt == IDFile.RevRefs.end())
      return DSError::DBInconsistent;
    for (const auto &V : RevRefListIt->second) {
      if (Remaining == 0)
        return true;
      --Remaining;
      auto RevRef = Snapshot->getMarshaller().toNative(V);
      if (!RevRef)
        return RevRef.getError();
      Callback(*RevRef);
    }
  }
  if (auto EC = It->errorCode())
    return EC;
  return false;
}

llvm::ErrorOr<bool> DocumentStoreQuery::fuzzyFind(
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
    auto CurOrErr =
        lmdb::Cursor::open(Snapshot->getTxn(), DB->IndexedSymbolsDbi);
    if (!CurOrErr)
      return CurOrErr.getError();
    Slice OidSlice;
    auto EC = CurOrErr->get(OidSlice, MDB_FIRST);
    if (EC && EC != lmdb::DBError::Notfound)
      return mapErrorCodeCommon(EC);
    Criteria.push_back(std::make_unique<SymbolsIterator>(std::move(*CurOrErr)));
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
    Slice ISymbolSlice;
    auto EC = DB->IndexedSymbolsDbi.get(Snapshot->getTxn(),
                                        makeSlice(SymbolDocID), ISymbolSlice);
    if (EC)
      return DSError::DBInconsistent;
    auto &IndexedSym = ISymbolSlice.toTypeRef<cl::IndexedSymbol>();
    ;
    const std::optional<float> Score =
        Filter.match(llvm::StringRef(IndexedSym.Sym.Name));
    if (!Score)
      continue;
    auto &[SerializedSym, Sym] = Symbols[SymbolDocID];
    SerializedSym = IndexedSym.Sym;
    auto Res = Snapshot->getMarshaller().toNative(SerializedSym);
    if (!Res)
      return Res.getError();
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

std::error_code DocumentStoreQuery::relations(
    const RelationsRequest &Req,
    llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback) const {
  uint32_t Remaining = Req.Limit.value_or(std::numeric_limits<uint32_t>::max());
  auto &Marshaller = Snapshot->getMarshaller();
  for (const auto &ID : Req.Subjects) {
    auto PredicateKey =
        cl::data::pair{Marshaller.fromNative(ID), Req.Predicate};
    auto It = Snapshot->getKeyToFileDbiIterator(DB->RelationKeyToIDFileDbi,
                                                makeSlice(PredicateKey));
    if (!It) {
      auto EC = It.getError();
      if (EC != DSError::Notfound)
        return EC;
      continue;
    }

    for (auto Res = It->peek(); Res; Res = It->next()) {
      const auto &IDFile = *Res->second;
      auto RelIt = IDFile.Relations.find(PredicateKey);
      if (RelIt == IDFile.Relations.end())
        return DSError::DBInconsistent;
      for (const auto &Rel : RelIt->second) {
        if (Remaining == 0)
          return {};
        --Remaining;
        auto SymOrErr = Snapshot->findSymbol(
            Snapshot->getMarshaller().toNative(Rel.Object));
        if (!SymOrErr)
          return SymOrErr.getError();
        auto Sym = Snapshot->getMarshaller().toNative(SymOrErr->Sym);
        if (!Sym)
          return Sym.getError();
        Callback(ID, *Sym);
      }
    }
    if (auto EC = It->errorCode())
      return EC;
  }
  return {};
}

std::unique_ptr<SymbolIndex>
createSymbolIndex(std::shared_ptr<DocumentStore> DB) {
  auto Inst = std::static_pointer_cast<DocumentStoreImpl>(DB);
  return std::make_unique<LMDBIndexImpl>(Inst);
}

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
