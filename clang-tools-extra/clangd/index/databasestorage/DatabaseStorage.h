//===--- DatabaseStorage.h - Dynamic on-disk symbol index. ------- C++-*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===--------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DATABASESTORAGE_DATABASESTORAGE_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DATABASESTORAGE_DATABASESTORAGE_H

#include "../../Path.h"
#include "../Index.h"
#include "lmdb/libraries/liblmdb/lmdb.h"
#include <mutex>

namespace clang {
namespace clangd {

class DatabaseStorage {
public:
  static std::unique_ptr<DatabaseStorage>
  open(PathRef DBPath, llvm::StringRef DBEngineName = "LMDB");

  virtual ~DatabaseStorage() = default;

  std::unique_ptr<SymbolIndex> getSymbolIndex() const;

  virtual bool
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback) const = 0;

  virtual void
  lookup(const LookupRequest &Req,
         llvm::function_ref<void(const Symbol &)> Callback) const = 0;

  virtual void refs(const RefsRequest &Req,
                    llvm::function_ref<void(const Ref &)> Callback) const = 0;

  virtual size_t estimateMemoryUsage() const { return 0; };

  /// \brief Update symbols in \p Path with symbols in \p Syms and references
  /// in \p Refs.
  virtual void update(PathRef File, std::unique_ptr<SymbolSlab> Syms,
                      std::unique_ptr<RefSlab> Refs) = 0;
};

class DatabaseIndex : public SymbolIndex {
public:
  DatabaseIndex(const DatabaseStorage *DBEngine) : DBEngine(DBEngine) {}

  bool
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback) const override {
    return DBEngine->fuzzyFind(Req, Callback);
  }

  void
  lookup(const LookupRequest &Req,
         llvm::function_ref<void(const Symbol &)> Callback) const override {
    return DBEngine->lookup(Req, Callback);
  }

  void refs(const RefsRequest &Req,
            llvm::function_ref<void(const Ref &)> Callback) const override {
    return DBEngine->refs(Req, Callback);
  }

  size_t estimateMemoryUsage() const override {
    return DBEngine->estimateMemoryUsage();
  };

private:
  const DatabaseStorage *DBEngine;
};

} // namespace clangd
} // namespace clang

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DATABASESTORAGE_DATABASESTORAGE_H