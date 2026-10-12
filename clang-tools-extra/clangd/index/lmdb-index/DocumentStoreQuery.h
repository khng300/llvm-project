//===--- DocumentStoreQuery.h - Symbol database -----------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_DOCUMENTSTOREQUERY_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_DOCUMENTSTOREQUERY_H

#include "DocumentStoreImpl.h"
#include "Iterator.h"

namespace clang {
namespace clangd {
namespace lmdb_index {

class DocumentStoreQuery {
public:
  static llvm::ErrorOr<DocumentStoreQuery> begin(DocumentStoreImpl &);

  std::unique_ptr<Iterator> iterator(const Token &Tok) const;
  // Constructs BOOST iterators for Path Proximities.
  std::unique_ptr<Iterator>
  createFileProximityIterator(llvm::ArrayRef<std::string> ProximityPaths) const;
  // Constructs BOOST iterators for preferred types.
  std::unique_ptr<Iterator>
  createTypeBoostingIterator(llvm::ArrayRef<std::string> Types) const;

  std::error_code
  lookup(const LookupRequest &Req,
         llvm::function_ref<void(const Symbol &)> Callback) const;

  llvm::ErrorOr<bool>
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback) const;

  llvm::ErrorOr<bool>
  refs(const RefsRequest &Req,
       llvm::function_ref<void(const Ref &)> Callback) const;

  llvm::ErrorOr<bool> containedRefs(
      const ContainedRefsRequest &Req,
      llvm::function_ref<void(const ContainedRefsResult &)> Callback) const;

  llvm::ErrorOr<bool>
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback);

  std::error_code
  relations(const RelationsRequest &Req,
            llvm::function_ref<void(const SymbolID &, const Symbol &)> Callback)
      const;

private:
  DocumentStoreQuery() : Corpus(0) {};

  std::shared_ptr<DocumentStoreImpl> DB;
  std::unique_ptr<TransactionImpl> Snapshot;
  lmdb_index::Corpus Corpus;
};

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
