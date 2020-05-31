#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_UTILS_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_UTILS_H

#include "../Symbol.h"
#include "llvm/Support/StringSaver.h"

namespace clang {
namespace clangd {
namespace dbindex {

/// Own \Sym by copying the strings inside to \p StringStore
static inline void ownSymbol(Symbol &Sym, llvm::UniqueStringSaver &Saver) {
  visitStrings(Sym, [&Saver](llvm::StringRef &Str) { Str = Saver.save(Str); });
}

} // namespace dbindex
} // namespace clangd
} // namespace clang

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_UTILS_H
