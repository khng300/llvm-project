//===--- Merge.h - Symbol database ------------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_MERGE_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_MERGE_H

#include "Serialization.h"

namespace clang {
namespace clangd {
namespace db_index {

cl::Symbol mergeSymbol(const cl::Symbol &L, const cl::Symbol &R);

} // namespace db_index
} // namespace clangd
} // namespace clang

#endif