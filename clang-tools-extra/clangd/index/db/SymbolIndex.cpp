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
namespace db_index {

LMDBIndexImpl::LMDBIndexImpl(std::shared_ptr<LMDBInstance> DB) : DB(DB) {}

void LMDBIndexImpl::lookup(
    const LookupRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  auto Query = LMDBInstance::QueryRequest::begin(*DB);
  if (!Query)
    return;
  Query->lookup(Req, Callback);
}

bool LMDBIndexImpl::refs(const RefsRequest &Req,
                         llvm::function_ref<void(const Ref &)> Callback) const {
  auto Query = LMDBInstance::QueryRequest::begin(*DB);
  if (!Query)
    return false;
  return Query->refs(Req, Callback);
}

bool LMDBIndexImpl::fuzzyFind(
    const FuzzyFindRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  auto Query = LMDBInstance::QueryRequest::begin(*DB);
  if (!Query)
    return false;
  return Query->fuzzyFind(Req, Callback);
}

void LMDBIndexImpl::relations(
    const RelationsRequest &Req,
    llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback) const {
  auto Query = LMDBInstance::QueryRequest::begin(*DB);
  if (!Query)
    return;
  Query->relations(Req, Callback);
}

size_t LMDBIndexImpl::estimateMemoryUsage() const { return 0; }

} // namespace db_index
} // namespace clangd
} // namespace clang