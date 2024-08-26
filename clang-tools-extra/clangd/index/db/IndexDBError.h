//===--- IndexDBError.h - Symbol database -----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_INDEXDBERROR_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_INDEXDBERROR_H

#include <system_error>

namespace clang {
namespace clangd {
namespace db_index {

enum class IndexDBError {
  Notfound = 1,
  LayoutMismatch,
  UnsupportedLayout,
  Corrupted,
  InternalError,
  MapFull,
  Last,
};

class IndexDBErrorCategory final : public std::error_category {
public:
  const char *name() const noexcept override;
  std::string message(int Condition) const override;
};

// NOLINTBEGIN(readability-identifier-naming)
std::error_code make_error_code(IndexDBError E);
// NOLINTEND(readability-identifier-naming)

} // namespace db_index
} // namespace clangd
} // namespace clang

namespace std {
template <>
struct is_error_code_enum<clang::clangd::db_index::IndexDBError>
    : std::true_type {};
} // namespace std

#endif