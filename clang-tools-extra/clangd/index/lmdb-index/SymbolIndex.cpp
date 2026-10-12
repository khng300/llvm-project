//===--- SymbolIndex.cpp - Symbol database ----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "SymbolIndex.h"

namespace clang {
namespace clangd {
namespace lmdb_index {

LMDBIndexImpl::LMDBIndexImpl(std::shared_ptr<DocumentStoreImpl> DB) : DB(DB) {}

void LMDBIndexImpl::lookup(
    const LookupRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  auto Query = DocumentStoreQuery::begin(*DB);
  if (!Query)
    return;
  auto EC = Query->lookup(Req, Callback);
  if (EC)
    DB->raiseFatalError(EC);
}

bool LMDBIndexImpl::refs(const RefsRequest &Req,
                         llvm::function_ref<void(const Ref &)> Callback) const {
  auto Query = DocumentStoreQuery::begin(*DB);
  if (!Query)
    return false;
  auto Result = Query->refs(Req, Callback);
  if (!Result)
    DB->raiseFatalError(Result.getError());
  return *Result;
}

bool LMDBIndexImpl::containedRefs(
    const ContainedRefsRequest &Req,
    llvm::function_ref<void(const ContainedRefsResult &)> Callback) const {
  auto Query = DocumentStoreQuery::begin(*DB);
  if (!Query)
    return false;
  auto Result = Query->containedRefs(Req, Callback);
  if (!Result)
    DB->raiseFatalError(Result.getError());
  return *Result;
}

bool LMDBIndexImpl::fuzzyFind(
    const FuzzyFindRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  auto Query = DocumentStoreQuery::begin(*DB);
  if (!Query)
    return false;
  auto Result = Query->fuzzyFind(Req, Callback);
  if (!Result)
    DB->raiseFatalError(Result.getError());
  return *Result;
}

void LMDBIndexImpl::relations(
    const RelationsRequest &Req,
    llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback) const {
  auto Query = DocumentStoreQuery::begin(*DB);
  if (!Query)
    return;
  auto EC = Query->relations(Req, Callback);
  if (EC)
    DB->raiseFatalError(EC);
}

size_t LMDBIndexImpl::estimateMemoryUsage() const { return 0; }

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
