//===--- VectorIterator.h - Symbol identifiers storage interface  --*- C++
//-*-===//
#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_VECTORITERATOR_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_VECTORITERATOR_H

#include "Iterator.h"
#include "llvm/ADT/ArrayRef.h"

namespace clang {
namespace clangd {
namespace dbindex {

/// \brief Query Tree Iterator implementation for std::vector
class VectorIterator : public Iterator {
public:
  VectorIterator(llvm::ArrayRef<DocID> V) : IDs(V), It(V.begin()) {}

  /// Check if the vector iterator reaches the end of vector
  bool reachedEnd() const override { return It == IDs.end(); }

  /// Advance the vector iterator to the next element
  void advance() override {
    assert(!reachedEnd() && "Vector iterator can't advance() at the end.");
    It++;
  }

  /// Advance the vector iterator to the next element with given ID
  void advanceTo(DocID ID) override {
    assert(!reachedEnd() && "Vector iterator can't advanceTo() at the end.");
    llvm::partition_point(llvm::make_range(It, IDs.end()),
                          [ID](DocID I) { return I < ID; });
  }

  /// Return the DocID under the vector iterator to the current element
  DocID peek() const override {
    assert(!reachedEnd() && "Vector iterator can't peek() at the end.");
    return *It;
  }

  float consume() override {
    assert(!reachedEnd() && "Vector iterator can't consume() at the end.");
    return 1;
  }

  size_t estimateSize() const override { return IDs.size(); }

private:
  llvm::raw_ostream &dump(llvm::raw_ostream &OS) const override {
    OS << '[';
    const char *Sep = "";
    for (const DocID Doc : IDs) {
      OS << Sep << Doc;
      Sep = " ";
    }
    return OS << ']';
  }

  llvm::ArrayRef<DocID> IDs;
  decltype(IDs)::iterator It;
};

} // namespace dbindex
} // namespace clangd
} // namespace clang

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_VECTORITERATOR_H
