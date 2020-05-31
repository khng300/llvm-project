//===--- LMDBIterator.h - Symbol identifiers storage interface --*- C++ -*-===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_LMDBITERATOR_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_LMDBITERATOR_H

#include "Iterator.h"
#include "LmdbCxx.h"
#include "llvm/ADT/StringRef.h"
#include <lmdb.h>

namespace clang {
namespace clangd {
namespace dbindex {

/// \brief Query Tree Iterator implementation for Lmdb
class LMDBIterator : public Iterator {
public:
  LMDBIterator(lmdb::Cursor &&Cur)
      : ErrorFlag(false), Cursor(std::move(Cur)), EndReached(false) {}

  /// Check if the vector iterator reaches the end of vector
  bool reachedEnd() const override {
    if (ErrorFlag || EndReached)
      return true;
    return false;
  }

  /// Advance the vector iterator to the next element
  void advance() override {
    assert(!reachedEnd() && "Vector iterator can't advance() at the end.");
    lmdb::Val K, D;
    auto Res = Cursor.get(K, D, MDB_NEXT_DUP);
    if (!Res) {
      llvm::consumeError(Res.takeError());
      ErrorFlag = true;
    } else if (Res.isHit())
      EndReached = true;
  }

  /// Advance the vector iterator to the next element with given ID
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() && "Vector iterator can't advanceTo() at the end.");
    lmdb::Val K, D;
    if (auto Err = Cursor.get(K, D, MDB_GET_CURRENT)) {
      ErrorFlag = true;
      return;
    }
    D = lmdb::Val(&ID);
    auto Res = Cursor.get(K, D, MDB_GET_BOTH_RANGE);
    if (!Res) {
      llvm::consumeError(Res.takeError());
      ErrorFlag = true;
    } else if (Res.isHit())
      EndReached = true;
  }

  /// Return the DocID under the vector iterator to the current element
  DocID peek() const override {
    lmdb::Val K, D;
    auto Res = Cursor.get(K, D, MDB_GET_CURRENT);
    if (!Res || !Res.isHit()) {
      ErrorFlag = true;
      return -1ull;
    }
    return *D.data<DocID>();
  }

  float consume() override {
    assert(!reachedEnd() && "Vector iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override {
    auto Count = Cursor.count();
    return Count ? *Count : 0;
  }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override { return OS; }

  mutable bool ErrorFlag;
  mutable lmdb::Cursor Cursor;
  bool EndReached;
};

} // namespace dbindex
} // namespace clangd
} // namespace clang

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_LMDBITERATOR_H
