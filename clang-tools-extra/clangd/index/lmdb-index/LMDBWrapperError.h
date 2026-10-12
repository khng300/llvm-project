//===--- LMDBWrapperError.h - LMDB Wrapper ----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_LMDBWRAPPERERROR_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_LMDBWRAPPERERROR_H

#include <system_error>

namespace lmdb {

enum class DBError : int {
  Keyexist = -30799,
  Notfound = -30798,
  PageNotfound = -30797,
  Corrupted = -30796,
  Panic = -30795,
  VersionMismatch = -30794,
  Invalid = -30793,
  MapFull = -30792,
  DbsFull = -30791,
  ReadersFull = -30790,
  TlsFull = -30789,
  TxnFull = -30788,
  CursorFull = -30787,
  PageFull = -30786,
  MapResized = -30785,
  Incompatible = -30784,
  BadRslot = -30783,
  BadTxn = -30782,
  BadValsize = -30781,
  BadDBI = -30780,
  Problem = -30779,
};

class DBErrorCategory final : public std::error_category {
public:
  const char *name() const noexcept override;
  std::string message(int Condition) const override;
};

// NOLINTBEGIN(readability-identifier-naming)
std::error_code make_error_code(lmdb::DBError E);
// NOLINTEND(readability-identifier-naming)

} // namespace lmdb

namespace std {
template <> struct is_error_code_enum<lmdb::DBError> : std::true_type {};
} // namespace std

#endif
