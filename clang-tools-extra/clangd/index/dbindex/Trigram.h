//===--- Trigram.h - Trigram generation for Fuzzy Matching ------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
///
/// \file
/// Trigrams are attributes of the symbol unqualified name used to effectively
/// extract symbols which can be fuzzy-matched given user query from the
/// inverted index. To match query with the extracted set of trigrams Q, the set
/// of generated trigrams T for identifier (unqualified symbol name) should
/// contain all items of Q, i.e. Q ⊆ T.
///
/// Trigram sets extracted from unqualified name and from query are different:
/// the set of query trigrams only contains consecutive sequences of three
/// characters (which is only a subset of all trigrams generated for an
/// identifier).
///
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_TRIGRAM_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_TRIGRAM_H

#include "Token.h"

#include <string>

namespace clang {
namespace clangd {
namespace dbindex {

/// Returns list of unique fuzzy-search trigrams from unqualified symbol.
/// The trigrams give the 3-character query substrings this symbol can match.
///
/// The symbol's name is broken into segments, e.g. "FooBar" has two segments.
/// Trigrams can start at any character in the input. Then we can choose to move
/// to the next character or to the start of the next segment.
///
/// Short trigrams (length 1-2) are used for short queries. These are:
///  - prefixes of the identifier, of length 1 and 2
///  - the first character + next head character
///
/// For "FooBar" we get the following trigrams:
///  {f, fo, fb, foo, fob, fba, oob, oba, bar}.
///
/// Trigrams are lowercase, as trigram matching is case-insensitive.
/// Trigrams in the returned list are deduplicated.
std::vector<Token> generateIdentifierTrigrams(llvm::StringRef Identifier);

/// Returns list of unique fuzzy-search trigrams given a query.
///
/// Query is segmented using FuzzyMatch API and downcasted to lowercase. Then,
/// the simplest trigrams - sequences of three consecutive letters and digits
/// are extracted and returned after deduplication.
///
/// For short queries (less than 3 characters with Head or Tail roles in Fuzzy
/// Matching segmentation) this returns a single trigram with the first
/// characters (up to 3) to perform prefix match.
std::vector<Token> generateQueryTrigrams(llvm::StringRef Query);

// Mark symbols which are can be used for code completion.
extern const Token RestrictedForCodeCompletion;

std::vector<std::string> generateProximityURIs(llvm::StringRef URIPath);

// Returns the tokens which are given symbol's characteristics. Currently, the
// generated tokens only contain fuzzy matching trigrams and symbol's scope,
// but in the future this will also return path proximity tokens and other
// types of tokens such as symbol type (if applicable).
// Returns the tokens which are given symbols's characteristics. For example,
// trigrams and scopes.
// FIXME(kbobyrev): Support more token types:
// * Namespace proximity
std::vector<Token> generateSearchTokens(const Symbol &Sym);

} // namespace dbindex
} // namespace clangd
} // namespace clang

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_TRIGRAM_H
