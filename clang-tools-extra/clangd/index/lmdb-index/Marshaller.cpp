//===--- Marshaller.cpp - Symbol database -----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "DocumentStoreImpl.h"
#include "Serialization.h"

#include <vector>

namespace clang {
namespace clangd {
namespace lmdb_index {

namespace {
const char *NonExistentFileURI = "<NONEXISTENT FILE>";
} // namespace

cl::SymbolID TransactionImpl::MarshallingHelper::fromNative(
    clangd::SymbolID V) {
  cl::SymbolID R;
  std::copy(V.raw().begin(), V.raw().end(), R.begin());
  return R;
}

clangd::SymbolID
TransactionImpl::MarshallingHelper::toNative(
    cl::SymbolID V) {
  return clangd::SymbolID::fromRaw(
      llvm::toStringRef(llvm::ArrayRef(V.data(), V.size())));
}

cl::FileDigest
TransactionImpl::MarshallingHelper::fromNative(
    clangd::FileDigest V) {
  cl::FileDigest R;
  std::copy(V.begin(), V.end(), R.begin());
  return R;
}

clangd::FileDigest
TransactionImpl::MarshallingHelper::toNative(
    cl::FileDigest V) {
  clangd::FileDigest R;
  std::copy(V.begin(), V.end(), R.begin());
  return R;
}

cl::SymbolLocation::Position
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::SymbolLocation::Position &V) {
  cl::SymbolLocation::Position R;
  R.setLine(V.line());
  R.setColumn(V.column());
  return R;
}

clangd::SymbolLocation::Position
TransactionImpl::MarshallingHelper::toNative(
    const cl::SymbolLocation::Position &V) {
  clangd::SymbolLocation::Position R;
  R.setLine(V.line());
  R.setColumn(V.column());
  return R;
}

llvm::ErrorOr<cl::SymbolLocation>
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::SymbolLocation &V) {
  cl::SymbolLocation R;
  R.Start = fromNative(V.Start);
  R.End = fromNative(V.End);
  if (auto FileURI = llvm::StringRef(V.FileURI); !FileURI.empty()) {
    auto Oid = Txn->ensureShardExistence(FileURI);
    if (!Oid)
      return Oid.getError();
    R.FileOid = *Oid;
  }
  return R;
}

llvm::ErrorOr<clangd::SymbolLocation>
TransactionImpl::MarshallingHelper::toNative(
    const cl::SymbolLocation &V) {
  clangd::SymbolLocation R;
  R.Start = toNative(V.Start);
  R.End = toNative(V.End);
  if (V.FileOid != OID_None) {
    auto IDFile = Txn->getIndexDataFile(V.FileOid);
    if (!IDFile) {
      auto EC = IDFile.getError();
      if (EC != DSError::Notfound)
        return EC;
      if (!Txn->isReadOnly())
        return DSError::DBInconsistent;
      R.FileURI = NonExistentFileURI;
    } else
      R.FileURI = (*IDFile)->FileURI.data();
  }
  return R;
}

cl::Symbol::IncludeHeaderWithReferences
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Symbol::IncludeHeaderWithReferences &V) {
  cl::Symbol::IncludeHeaderWithReferences R;
  R.IncludeHeader = V.IncludeHeader;
  R.References = V.References;
  R.SupportedDirectives = V.supportedDirectives();
  return R;
}

clangd::Symbol::IncludeHeaderWithReferences
TransactionImpl::MarshallingHelper::toNative(
    const cl::Symbol::IncludeHeaderWithReferences &V) {
  clangd::Symbol::IncludeHeaderWithReferences R;
  R.IncludeHeader = V.IncludeHeader.view();
  R.References = V.References;
  R.SupportedDirectives = V.SupportedDirectives;
  return R;
}

llvm::ErrorOr<cl::Symbol>
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Symbol &V) {
  cl::Symbol R;
  R.ID = fromNative(V.ID);
  R.SymInfo = V.SymInfo;
  R.Name = V.Name;
  R.Scope = V.Scope;
  if (V.Definition) {
    auto Definition = fromNative(V.Definition);
    if (!Definition)
      return Definition.getError();
    R.Definition = *Definition;
  }
  if (V.CanonicalDeclaration) {
    auto CanonicalDeclaration = fromNative(V.CanonicalDeclaration);
    if (!CanonicalDeclaration)
      return CanonicalDeclaration.getError();
    R.CanonicalDeclaration = *CanonicalDeclaration;
  }
  R.References = V.References;
  R.Origin = V.Origin;
  R.Signature = V.Signature;
  R.TemplateSpecializationArgs = V.TemplateSpecializationArgs;
  R.CompletionSnippetSuffix = V.CompletionSnippetSuffix;
  R.Documentation = V.Documentation;
  R.ReturnType = V.ReturnType;
  R.Type = V.Type;
  for (auto &VV : V.IncludeHeaders)
    R.IncludeHeaders.push_back(fromNative(VV));
  R.Flags = V.Flags;
  return R;
}

llvm::ErrorOr<clangd::Symbol>
TransactionImpl::MarshallingHelper::toNative(
    const cl::Symbol &V) {
  clangd::Symbol R;
  R.ID = toNative(V.ID);
  R.SymInfo = V.SymInfo;
  R.Name = V.Name.view();
  R.Scope = V.Scope.view();
  if (V.Definition) {
    auto Definition = toNative(V.Definition);
    if (!Definition)
      return Definition.getError();
    R.Definition = *Definition;
  }
  if (V.CanonicalDeclaration) {
    auto CanonicalDeclaration = toNative(V.CanonicalDeclaration);
    if (!CanonicalDeclaration)
      return CanonicalDeclaration.getError();
    R.CanonicalDeclaration = *CanonicalDeclaration;
  }
  R.References = V.References;
  R.Origin = V.Origin;
  R.Signature = V.Signature.view();
  R.TemplateSpecializationArgs = V.TemplateSpecializationArgs.view();
  R.CompletionSnippetSuffix = V.CompletionSnippetSuffix.view();
  R.Documentation = V.Documentation.view();
  R.ReturnType = V.ReturnType.view();
  R.Type = V.Type.view();
  for (auto &V : V.IncludeHeaders)
    R.IncludeHeaders.push_back(toNative(V));
  R.Flags = V.Flags;
  return R;
}

llvm::ErrorOr<cl::Ref>
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Ref &V) {
  cl::Ref R;
  auto Location = fromNative(V.Location);
  if (!Location)
    return Location.getError();
  R.Location = *Location;
  R.Kind = V.Kind;
  R.Container = fromNative(V.Container);
  return R;
}

llvm::ErrorOr<clangd::Ref>
TransactionImpl::MarshallingHelper::toNative(
    const cl::Ref &V) {
  clangd::Ref R;
  auto Location = toNative(V.Location);
  if (!Location)
    return Location.getError();
  R.Location = *Location;
  R.Kind = V.Kind;
  R.Container = toNative(V.Container);
  return R;
}

cl::Relation TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Relation &V) {
  cl::Relation R;
  R.Subject = fromNative(V.Subject);
  R.Predicate = V.Predicate;
  R.Object = fromNative(V.Object);
  return R;
}

clangd::Relation
TransactionImpl::MarshallingHelper::toNative(
    const cl::Relation &V) {
  clangd::Relation R;
  R.Subject = toNative(V.Subject);
  R.Predicate = V.Predicate;
  R.Object = toNative(V.Object);
  return R;
}

llvm::ErrorOr<cl::RevRef>
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::ContainedRefsResult &V) {
  cl::RevRef R;
  if (auto Res = fromNative(V.Location))
    R.Location = *Res;
  else
    return Res.getError();
  R.Kind = V.Kind;
  R.Symbol = fromNative(V.Symbol);
  return R;
}

llvm::ErrorOr<clangd::ContainedRefsResult>
TransactionImpl::MarshallingHelper::toNative(
    const cl::RevRef &V) {
  clangd::ContainedRefsResult R;
  if (auto Res = toNative(V.Location))
    R.Location = *Res;
  else
    return Res.getError();
  R.Kind = V.Kind;
  R.Symbol = toNative(V.Symbol);
  return R;
}

cl::IncludeGraphNode
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::IncludeGraphNode &V) {
  cl::IncludeGraphNode R;
  R.Flags = V.Flags;
  R.URI = V.URI;
  R.Digest = fromNative(V.Digest);
  for (const auto &VV : V.DirectIncludes)
    R.DirectIncludes.emplace_back(VV);
  return R;
}

clangd::IncludeGraphNode
TransactionImpl::MarshallingHelper::toNative(
    const cl::IncludeGraphNode &V) {
  clangd::IncludeGraphNode R;
  R.Flags = V.Flags;
  R.URI = V.URI.view();
  R.Digest = V.Digest;
  for (const auto &VV : V.DirectIncludes)
    R.DirectIncludes.emplace_back(VV);
  return R;
}

cl::IncludeGraph
TransactionImpl::MarshallingHelper::fromNative(
    const clangd::IncludeGraph &V) {
  cl::IncludeGraph R;
  for (const auto &VV : V)
    R[cl::data::cstring(VV.first())] = fromNative(VV.second);
  return R;
}

clangd::IncludeGraph
TransactionImpl::MarshallingHelper::toNative(
    const cl::IncludeGraph &V) {
  clangd::IncludeGraph R;
  for (const auto &VV : V) {
    auto It = R.insert_or_assign(VV.first.view(), toNative(VV.second)).first;
    It->getValue().URI = It->getKey();
    for (auto &V : It->second.DirectIncludes)
      V = R.try_emplace(V).first->getKey();
  }
  return R;
}

cl::CompileCommand
TransactionImpl::MarshallingHelper::fromNative(
    const tooling::CompileCommand &V) {
  cl::CompileCommand R;
  R.Directory = V.Directory;
  R.Filename = V.Filename;
  for (const auto &VV : V.CommandLine)
    R.CommandLine.emplace_back(VV);
  R.Output = V.Output;
  R.Heuristic = V.Heuristic;
  return R;
}

tooling::CompileCommand
TransactionImpl::MarshallingHelper::toNative(
    const cl::CompileCommand &V) {
  tooling::CompileCommand R;
  R.Directory = V.Directory;
  R.Filename = V.Filename;
  for (const auto &VV : V.CommandLine)
    R.CommandLine.emplace_back(VV);
  R.Output = V.Output;
  R.Heuristic = V.Heuristic;
  return R;
}

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
