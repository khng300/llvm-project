//===--- SymbolIndex.h - Symbol database ------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_SYMBOLINDEX_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_SYMBOLINDEX_H

#include "DocumentStore.h"
#include "index/Index.h"

namespace clang {
namespace clangd {
namespace db_index {

class LMDBIndexImpl : public SymbolIndex {
public:
  LMDBIndexImpl(std::shared_ptr<LMDBInstance>);

  bool
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback) const override;

  void lookup(const LookupRequest &Req,
              llvm::function_ref<void(const Symbol &)> Callback) const override;

  bool refs(const RefsRequest &Req,
            llvm::function_ref<void(const Ref &)> Callback) const override;

  void relations(const RelationsRequest &Req,
                 llvm::function_ref<void(const SymbolID &, const Symbol &)>
                     Callback) const override;

  llvm::unique_function<IndexContents(llvm::StringRef) const>
  indexedFiles() const override {
    return [](llvm::StringRef) { return IndexContents::All; };
  }

  size_t estimateMemoryUsage() const override;

private:
  std::shared_ptr<LMDBInstance> DB;
};

} // namespace db_index
} // namespace clangd
} // namespace clang

#endif