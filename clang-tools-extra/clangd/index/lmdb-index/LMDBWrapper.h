//===--- LMDBWrapper.h - LMDB Wrapper ---------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_LMDBWRAPPER_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_LMDBWRAPPER_H

#include "LMDBWrapperError.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Support/ErrorOr.h"

#include "lmdb.h"
#include <system_error>

namespace lmdb {

class Slice {
  const void *Data = nullptr;
  size_t Size = 0;

public:
  constexpr Slice() noexcept = default;
  constexpr Slice(const void *Ptr, size_t N) noexcept : Data(Ptr), Size(N) {}
  constexpr Slice(const MDB_val &Val) noexcept
      : Data(Val.mv_data), Size(Val.mv_size) {}
  constexpr Slice(std::string_view Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  constexpr Slice(llvm::StringRef Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  constexpr Slice(const std::string &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  template <size_t N>
  constexpr Slice(const llvm::SmallString<N> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  constexpr Slice(const std::vector<char> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  constexpr Slice(const std::vector<unsigned char> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  template <size_t N>
  constexpr Slice(const llvm::SmallVector<char, N> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  template <size_t N>
  constexpr Slice(const llvm::SmallVector<unsigned char, N> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  template <size_t N>
  constexpr Slice(const std::array<char, N> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  template <size_t N>
  constexpr Slice(const std::array<unsigned char, N> &Str) noexcept
      : Data(Str.data()), Size(Str.size()) {}
  template <size_t N>
  constexpr Slice(const char Str[N]) noexcept : Data(Str), Size(sizeof(Str)) {}
  template <size_t N>
  constexpr Slice(const unsigned char Str[N]) noexcept
      : Data(Str), Size(sizeof(Str)) {}

  int compare(const Slice &RHS) const {
    int Res = memcmp(Data, RHS.Data, std::min(Size, RHS.Size));
    if (Res == 0)
      Res = Size == RHS.Size ? 0 : (Size < RHS.Size ? -1 : 1);
    return Res;
  }

  constexpr const void *data() const noexcept { return Data; }
  constexpr size_t size() const noexcept { return Size; }

  bool operator==(const Slice &RHS) const { return compare(RHS) == 0; }
  bool operator<(const Slice &RHS) const { return compare(RHS) < 0; }
  bool operator<=(const Slice &RHS) const { return compare(RHS) <= 0; }
  bool operator>(const Slice &RHS) const { return compare(RHS) > 0; }
  bool operator>=(const Slice &RHS) const { return compare(RHS) >= 0; }

  operator MDB_val() const { return MDB_val{Size, const_cast<void *>(Data)}; }

  operator llvm::StringRef() const {
    return llvm::StringRef(static_cast<const char *>(Data), Size);
  }

  operator std::string_view() const {
    return std::string_view(static_cast<const char *>(Data), Size);
  }

  operator llvm::ArrayRef<unsigned char>() const {
    return llvm::ArrayRef(static_cast<const unsigned char *>(Data), Size);
  }

  template <typename T> const T &toTypeRef() {
    assert(Size >= sizeof(T));
    return *reinterpret_cast<const T *>(Data);
  }

  const unsigned char *bytesBegin() const {
    return static_cast<const unsigned char *>(Data);
  }
  const unsigned char *bytesEnd() const { return bytesBegin() + Size; }

  std::vector<unsigned char> vec() const {
    return std::vector<unsigned char>(bytesBegin(), bytesEnd());
  }
};

template <typename Type> Slice makeSlice(const Type &O) {
  return Slice(&O, sizeof(Type));
}

class Environment {
  class Key {
    friend class Environment;
    friend class Txn;

    Key() {}
    Key(const Key &) = default;
  };

  void moveAssign(Environment &&Env) noexcept;

public:
  static constexpr unsigned int DefaultFlags = 0;
  static constexpr ::mdb_mode_t DefaultMode = 0644;

  Environment() = default;
  Environment(const Environment &Env) = delete;
  Environment(Environment &&Env) { moveAssign(std::move(Env)); }
  ~Environment() { close(); }

  Environment &operator=(const Environment &Env) = delete;
  Environment &operator=(Environment &&Env) noexcept {
    moveAssign(std::move(Env));
    return *this;
  }

  static llvm::ErrorOr<Environment> create(unsigned int Flags = DefaultFlags);
  void close() noexcept;

  std::error_code setFlags(unsigned int Flags, bool Set);
  std::error_code setMapSize(std::size_t MapSize);
  std::error_code setMaxDBs(MDB_dbi Count);
  std::error_code setMaxReaders(unsigned int Count);
  ::MDB_envinfo getEnvInfo();

  std::error_code open(std::string_view Path, unsigned int Flags,
                       ::mdb_mode_t Mode = DefaultMode);

  MDB_env *handle(const Key &) const { return Handle; }

private:
  Environment(MDB_env *Handle) : Handle(Handle) {}

  MDB_env *Handle = nullptr;
};

class Txn {
  class Key {
    friend class Txn;
    friend class DBI;
    friend class Cursor;

    Key() {}
    Key(const Key &) = default;
  };

  Txn(MDB_txn *Handle, unsigned int Flags) : Handle(Handle), Flags(Flags) {}

  void moveAssign(Txn &&Tx) noexcept;

  void close() noexcept;

  MDB_txn *Handle = nullptr;
  unsigned int Flags = 0;

public:
  static constexpr unsigned int DefaultFlags = 0;

  Txn() = default;
  Txn(const Txn &Tx) = delete;
  Txn(Txn &&Tx) { moveAssign(std::move(Tx)); }
  ~Txn() { close(); }

  Txn &operator=(const Txn &Tx) = delete;
  Txn &operator=(Txn &&Tx) noexcept {
    moveAssign(std::move(Tx));
    return *this;
  }

  MDB_txn *handle(const Key &) const { return Handle; }

  static llvm::ErrorOr<Txn> begin(Environment &Env,
                                  unsigned int Flags = DefaultFlags);
  void reset();

  unsigned int flags() const { return Flags; }

  mdb_size_t txnID();

  std::error_code commit();
  void abort() noexcept;
};

class DBI {
  class Key {
    friend class DBI;
    friend class Cursor;

    Key() {}
    Key(const Key &) = default;
  };

public:
  static constexpr unsigned int DefaultFlags = 0;

  DBI() = default;

  MDB_dbi handle(const Key &) const { return Handle; }

  static llvm::ErrorOr<DBI> open(Txn &Tx, std::string_view Name,
                                 unsigned int Flags = DefaultFlags);
  llvm::ErrorOr<MDB_stat> stat(Txn &Tx) const;
  llvm::ErrorOr<unsigned int> flags(Txn &Tx) const;
  std::error_code setCompare(Txn &Tx, ::MDB_cmp_func *Cmp = nullptr);
  std::error_code get(Txn &Tx, Slice Key, Slice &Data);
  std::error_code put(Txn &Tx, Slice Key, Slice &Data, unsigned int Flags = 0);
  std::error_code del(Txn &Tx, Slice Key, Slice Data);
  std::error_code del(Txn &Tx, Slice Key);
  std::error_code drop(Txn &Tx, const bool Del = false);

private:
  DBI(MDB_dbi Handle) : Handle(Handle) {}

  MDB_dbi Handle = -1;
};

class Cursor {
  Cursor(MDB_cursor *Handle) : Handle(Handle) {}

  void moveAssign(Cursor &&) noexcept;

  MDB_cursor *Handle = nullptr;

public:
  Cursor() = default;
  Cursor(const Cursor &) = delete;
  Cursor(Cursor &&Cur) { moveAssign(std::move(Cur)); }
  ~Cursor() noexcept { close(); }

  Cursor &operator=(const Cursor &) = delete;
  Cursor &operator=(Cursor &&Cur) noexcept {
    moveAssign(std::move(Cur));
    return *this;
  }

  bool operator==(const Cursor &RHS) const { return Handle == RHS.Handle; }
  bool operator!=(const Cursor &RHS) const { return !(operator==(RHS.Handle)); }

  static llvm::ErrorOr<Cursor> open(MDB_txn *Tx, MDB_dbi Dbi);
  static llvm::ErrorOr<Cursor> open(Txn &Tx, DBI &Dbi) {
    return open(Tx.handle({}), Dbi.handle({}));
  }
  llvm::ErrorOr<Cursor> dup();
  void close() noexcept;

  std::error_code renew(Txn &Tx);
  MDB_txn *txn() const;
  MDB_dbi dbi() const;

  std::error_code get(Slice &Key, Slice &Data, ::MDB_cursor_op Operation);
  std::error_code get(Slice &Key, ::MDB_cursor_op Operation);
  std::error_code put(Slice Key, Slice &Data, unsigned int Flags = 0);
  std::error_code del(unsigned int Flags = 0);

  llvm::ErrorOr<::mdb_size_t> count();

  bool valid() { return Handle != nullptr; };
};

class DupIterator : lmdb::Cursor {
  std::error_code ErrorCode;

protected:
  std::optional<Slice> opImpl(::MDB_cursor_op Op);

public:
  DupIterator(lmdb::Cursor &&Cur) : lmdb::Cursor(std::move(Cur)) {}

  lmdb::Cursor &&getCursor() && {
    return std::move(static_cast<lmdb::Cursor &>(*this));
  }

  std::error_code errorCode() const { return ErrorCode; }

  llvm::ErrorOr<size_t> count();

  std::optional<Slice> seek(Slice Data);
  std::optional<Slice> peek() { return opImpl(MDB_GET_CURRENT); }
  std::optional<Slice> prev() { return opImpl(MDB_PREV_DUP); }
  std::optional<Slice> next() { return opImpl(MDB_NEXT_DUP); }
};

} // namespace lmdb

#endif
