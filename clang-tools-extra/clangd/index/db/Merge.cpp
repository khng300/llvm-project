//===--- Merge.cpp - Symbol database ----------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "Merge.h"

namespace clang {
namespace clangd {
namespace db_index {

namespace {

// Returns true if \p L is (strictly) preferred to \p R (e.g. by file paths). If
// neither is preferred, this returns false.
bool prefer(const cl::SymbolLocation &L, const cl::SymbolLocation &R) {
  if (!L)
    return false;
  if (!R)
    return true;
  auto HasCodeGenSuffix = [](const cl::SymbolLocation &Loc) {
    constexpr static const char *CodegenSuffixes[] = {".proto"};
    return llvm::any_of(CodegenSuffixes, [&](llvm::StringRef Suffix) {
      return llvm::StringRef(Loc.FileURI).ends_with(Suffix);
    });
  };
  return HasCodeGenSuffix(L) && !HasCodeGenSuffix(R);
}

} // namespace

cl::Symbol mergeSymbol(const cl::Symbol &L, const cl::Symbol &R) {
  assert(L.ID == R.ID);
  // We prefer information from TUs that saw the definition.
  // Classes: this is the def itself. Functions: hopefully the header decl.
  // If both did (or both didn't), continue to prefer L over R.
  bool PreferR = R.Definition && !L.Definition;
  // Merge include headers only if both have definitions or both have no
  // definition; otherwise, only accumulate references of common includes.
  assert(L.Definition.FileURI && R.Definition.FileURI);
  bool MergeIncludes =
      L.Definition.FileURI.empty() == R.Definition.FileURI.empty();
  cl::Symbol S = PreferR ? R : L; // The target symbol we're merging into.
  const cl::Symbol &O = PreferR ? L : R; // The "other" less-preferred symbol.

  // Only use locations in \p O if it's (strictly) preferred.
  if (prefer(O.CanonicalDeclaration, S.CanonicalDeclaration))
    S.CanonicalDeclaration = O.CanonicalDeclaration;
  if (prefer(O.Definition, S.Definition))
    S.Definition = O.Definition;
  S.References += O.References;
  if (S.Signature == "")
    S.Signature = O.Signature;
  if (S.CompletionSnippetSuffix == "")
    S.CompletionSnippetSuffix = O.CompletionSnippetSuffix;
  if (S.Documentation == "") {
    // Don't accept documentation from bare forward class declarations, if there
    // is a definition and it didn't provide one. S is often an undocumented
    // class, and O is a non-canonical forward decl preceded by an irrelevant
    // comment.
    bool IsClass = S.SymInfo.Kind == index::SymbolKind::Class ||
                   S.SymInfo.Kind == index::SymbolKind::Struct ||
                   S.SymInfo.Kind == index::SymbolKind::Union;
    if (!IsClass || !S.Definition)
      S.Documentation = O.Documentation;
  }
  if (S.ReturnType == "")
    S.ReturnType = O.ReturnType;
  if (S.Type == "")
    S.Type = O.Type;
  for (const auto &OI : O.IncludeHeaders) {
    bool Found = false;
    for (auto &SI : S.IncludeHeaders) {
      if (SI.IncludeHeader == OI.IncludeHeader) {
        Found = true;
        SI.References += OI.References;
        SI.SupportedDirectives |= OI.SupportedDirectives;
        break;
      }
    }
    if (!Found && MergeIncludes)
      S.IncludeHeaders.emplace_back(OI.IncludeHeader, OI.References,
                                    OI.SupportedDirectives);
  }

  S.Origin |= O.Origin | SymbolOrigin::Merge;
  S.Flags |= O.Flags;
  return S;
}

} // namespace db_index
} // namespace clangd
} // namespace clang