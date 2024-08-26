//===--- IndexDB.cpp - Symbol database --------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "IndexDB.h"

namespace clang {
namespace clangd {
namespace db_index {
namespace {

std::string IndexErrorString[static_cast<size_t>(IndexDBError::Last)] = {
    "Success",
    "Object not found",
    "Indexing layout mismatch",
    "Unsupported layout",
    "Corrupted",
    "Internal error",
    "Database map full",
};

IndexDBErrorCategory IndexErrCategory;

} // namespace

const char *IndexDBErrorCategory::name() const noexcept {
  return "indexdb.error";
}

std::string IndexDBErrorCategory::message(int Condition) const {
  if (Condition >= static_cast<int>(IndexDBError::Last))
    return "???";
  return IndexErrorString[Condition];
}

// NOLINTBEGIN(readability-identifier-naming)
std::error_code make_error_code(IndexDBError E) {
  return std::error_code(static_cast<int>(E), IndexErrCategory);
}
// NOLINTEND(readability-identifier-naming)

std::error_code mapDBErrorToIndexDBError(std::error_code EC) {
  if (EC.category() == IndexErrCategory)
    return EC;
  if (EC == lmdb::DBError::MapFull)
    return IndexDBError::MapFull;
  if (EC == lmdb::DBError::Notfound)
    return IndexDBError::Notfound;
  return EC;
}

} // namespace db_index
} // namespace clangd
} // namespace clang