//===--- Marshaller.cpp - Symbol database -----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "DocumentStore.h"
#include "Serialization.h"

#include <optional>
#include <vector>

namespace clang {
namespace clangd {
namespace db_index {

cl::SymbolID LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    clangd::SymbolID V) {
  cl::SymbolID R;
  std::copy(V.raw().begin(), V.raw().end(), R.begin());
  return R;
}

clangd::SymbolID
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(cl::SymbolID V) {
  return clangd::SymbolID::fromRaw(
      llvm::toStringRef(llvm::ArrayRef(V.data(), V.size())));
}

cl::FileDigest LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    clangd::FileDigest V) {
  cl::FileDigest R;
  std::copy(V.begin(), V.end(), R.begin());
  return R;
}

clangd::FileDigest
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(cl::FileDigest V) {
  clangd::FileDigest R;
  std::copy(V.begin(), V.end(), R.begin());
  return R;
}

cl::SymbolLocation::Position
LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::SymbolLocation::Position &V) {
  cl::SymbolLocation::Position R;
  R.Line = V.line();
  R.Column = V.column();
  return R;
}

clangd::SymbolLocation::Position
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
    const cl::SymbolLocation::Position &V) {
  clangd::SymbolLocation::Position R;
  R.setLine(V.Line);
  R.setColumn(V.Column);
  return R;
}

cl::SymbolLocation LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::SymbolLocation &V) {
  cl::SymbolLocation R;
  R.Start = fromNative(V.Start);
  R.End = fromNative(V.End);
  R.FileURI = V.FileURI;
  return R;
}

clangd::SymbolLocation
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
    const cl::SymbolLocation &V) {
  clangd::SymbolLocation R;
  R.Start = toNative(V.Start);
  R.End = toNative(V.End);
  R.FileURI = V.FileURI.data();
  return R;
}

cl::Symbol::IncludeHeaderWithReferences
LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Symbol::IncludeHeaderWithReferences &V) {
  cl::Symbol::IncludeHeaderWithReferences R;
  R.IncludeHeader = V.IncludeHeader;
  R.References = V.References;
  R.SupportedDirectives = V.supportedDirectives();
  return R;
}

clangd::Symbol::IncludeHeaderWithReferences
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
    const cl::Symbol::IncludeHeaderWithReferences &V) {
  clangd::Symbol::IncludeHeaderWithReferences R;
  R.IncludeHeader = V.IncludeHeader.view();
  R.References = V.References;
  R.SupportedDirectives = V.SupportedDirectives;
  return R;
}

cl::Symbol LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Symbol &V) {
  cl::Symbol R;
  R.ID = fromNative(V.ID);
  R.SymInfo = V.SymInfo;
  R.Name = V.Name;
  R.Scope = V.Scope;
  R.Definition = fromNative(V.Definition);
  R.CanonicalDeclaration = fromNative(V.CanonicalDeclaration);
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

clangd::Symbol LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
    const cl::Symbol &V) {
  clangd::Symbol R;
  R.ID = toNative(V.ID);
  R.SymInfo = V.SymInfo;
  R.Name = V.Name.view();
  R.Scope = V.Scope.view();
  R.Definition = toNative(V.Definition);
  R.CanonicalDeclaration = toNative(V.CanonicalDeclaration);
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

cl::Ref LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Ref &V) {
  cl::Ref R;
  R.Location = fromNative(V.Location);
  R.Kind = V.Kind;
  R.Container = fromNative(V.Container);
  return R;
}

clangd::Ref
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(const cl::Ref &V) {
  clangd::Ref R;
  R.Location = toNative(V.Location);
  R.Kind = V.Kind;
  R.Container = toNative(V.Container);
  return R;
}

cl::Relation LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::Relation &V) {
  cl::Relation R;
  R.Subject = fromNative(V.Subject);
  R.Predicate = V.Predicate;
  R.Object = fromNative(V.Object);
  return R;
}

clangd::Relation LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
    const cl::Relation &V) {
  clangd::Relation R;
  R.Subject = toNative(V.Subject);
  R.Predicate = V.Predicate;
  R.Object = toNative(V.Object);
  return R;
}

cl::IncludeGraphNode
LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
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
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
    const cl::IncludeGraphNode &V) {
  clangd::IncludeGraphNode R;
  R.Flags = V.Flags;
  R.URI = V.URI.view();
  R.Digest = V.Digest;
  for (const auto &VV : V.DirectIncludes)
    R.DirectIncludes.emplace_back(VV);
  return R;
}

cl::IncludeGraph LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
    const clangd::IncludeGraph &V) {
  cl::IncludeGraph R;
  for (const auto &VV : V)
    R[cl::data::cstring(VV.first())] = fromNative(VV.second);
  return R;
}

clangd::IncludeGraph LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
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

cl::CompileCommand LMDBInstance::TransactionImpl::MarshallingHelper::fromNative(
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
LMDBInstance::TransactionImpl::MarshallingHelper::toNative(
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

cl::IndexFileInfo
LMDBInstance::TransactionImpl::MarshallingHelper::toIndexFileInfo(
    llvm::StringRef ShardPath, const IndexFileInfo &V) {
  URI Uri = URI::createFile(ShardPath);
  cl::IndexFileInfo R;
  if (V.Cmd)
    R.Cmd = fromNative(*V.Cmd);
  R.ShardPath = ShardPath;
  R.FileUri = Uri.toString();
  R.ModifiedTime = V.ModifiedTime;
  if (V.Sources)
    R.Sources = fromNative(*V.Sources);
  R.Valid = true;
  return R;
}

cl::IndexFileInfo
LMDBInstance::TransactionImpl::MarshallingHelper::toIndexFileInfo(
    llvm::StringRef ShardPath, const IndexFileOut &V, int64_t ModifiedTime) {
  URI Uri = URI::createFile(ShardPath);
  cl::IndexFileInfo R;
  if (V.Cmd)
    R.Cmd = fromNative(*V.Cmd);
  R.ShardPath = ShardPath;
  R.FileUri = Uri.toString();
  R.ModifiedTime = ModifiedTime;
  if (V.Sources)
    R.Sources = fromNative(*V.Sources);
  R.Valid = true;
  return R;
}

cl::IndexFileInfo
LMDBInstance::TransactionImpl::MarshallingHelper::toEmptyIndexFileInfo(
    llvm::StringRef ShardPath) {
  URI Uri = URI::createFile(ShardPath);
  cl::IndexFileInfo R;
  R.ShardPath = ShardPath;
  R.FileUri = Uri.toString();
  return R;
}

IndexFileInfo
LMDBInstance::TransactionImpl::MarshallingHelper::fromIndexFileInfo(
    const cl::IndexFileInfo *V) {
  IndexFileInfo R;
  R.FileURI = V->FileUri;
  R.ShardPath = V->ShardPath;
  if (V->Cmd.has_value())
    R.Cmd = toNative(*V->Cmd);
  R.ModifiedTime = V->ModifiedTime;
  if (V->Sources.has_value())
    R.Sources.emplace() = toNative(*V->Sources);
  return R;
}

} // namespace db_index
} // namespace clangd
} // namespace clang