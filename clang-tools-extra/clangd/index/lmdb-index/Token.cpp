//===--- Token.cpp - Token support ------------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "Token.h"
#include "Trigram.h"
#include "URI.h"
#include "llvm/ADT/StringSet.h"
#include "llvm/Support/Path.h"

namespace clang {
namespace clangd {
namespace lmdb_index {

// Mark symbols which are can be used for code completion.
const Token RestrictedForCodeCompletion =
    Token(Token::Kind::Sentinel, "Restricted For Code Completion");

// Helper to efficiently assemble the inverse index (token -> matching docs).
// The output is a nice uniform structure keyed on Token, but constructing
// the Token object every time we want to insert into the map is wasteful.
// Instead we have various maps keyed on things that are cheap to compute,
// and produce the Token keys once at the end.
std::vector<Token> buildTokens(const Symbol &Sym) {
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
  [&](const Symbol &Sym) {
    generateIdentifierTrigrams(std::string_view(Sym.Name), TrigramScratch);
    for (Trigram T : TrigramScratch)
      TrigramDocs.insert(T);
    ScopeDocs.insert(std::string_view(Sym.Scope));
    if (!llvm::StringRef(Sym.CanonicalDeclaration.FileURI).empty())
      for (const auto &ProximityURI :
           generateProximityURIs(Sym.CanonicalDeclaration.FileURI)) {
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

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
