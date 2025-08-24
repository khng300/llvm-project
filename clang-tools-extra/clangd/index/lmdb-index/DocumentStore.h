//===--- DocumentStore.h - Symbol database ----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_LMDBENGINE_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_LMDBENGINE_H

#include "Headers.h"
#include "index/Index.h"
#include "index/Serialization.h"

#include <memory>

namespace clang {
namespace clangd {
namespace lmdb_index {

struct IndexFileInfo {
  std::string FileURI;
  std::string ShardPath;
  int64_t ModifiedTime;
  std::optional<tooling::CompileCommand> Cmd;
  std::optional<IncludeGraph> Sources;
};

struct Transaction {
  virtual ~Transaction() {}
  virtual std::error_code commit() && = 0;
  virtual void abort() && = 0;
  virtual std::error_code updateIndexedSymbols() = 0;
};

struct DocumentStore : std::enable_shared_from_this<DocumentStore> {
  virtual ~DocumentStore() {};

  virtual llvm::ErrorOr<std::unique_ptr<Transaction>>
  beginTransaction(bool RO, llvm::StringRef Tag = {}) = 0;

  virtual size_t databaseMapSize() = 0;
  virtual std::error_code growDatabaseMapSize() = 0;

  virtual std::unique_ptr<IndexFileInfo>
  loadShardInfo(Transaction &Tx, llvm::StringRef ShardPath) = 0;

  virtual std::error_code updateShard(Transaction &Tx,
                                      llvm::StringRef ShardPath,
                                      const IndexFileOut *Shard,
                                      int64_t ModifiedTime) = 0;
};

std::shared_ptr<DocumentStore>
createSharedDatabase(llvm::StringRef ProjectPath);
std::unique_ptr<SymbolIndex>
createSymbolIndex(std::shared_ptr<DocumentStore> DB);

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
