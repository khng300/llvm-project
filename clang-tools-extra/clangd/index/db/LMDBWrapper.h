//===--- LMDBWrapper.h - LMDB Wrapper ---------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_LMDBWRAPPER_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_LMDBWRAPPER_H

#include "LMDBErrors.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Support/ErrorOr.h"

#include "lmdb.h"
#include <system_error>

namespace lmdb {

inline MDB_val toVal(llvm::StringRef SV) {
  MDB_val Val;
  Val.mv_size = SV.size();
  Val.mv_data = const_cast<char *>(SV.data());
  return Val;
}

inline llvm::StringRef toStringRef(MDB_val Val) {
  return llvm::StringRef(static_cast<const char *>(Val.mv_data), Val.mv_size);
}

enum class DBOperation : int {
  First = MDB_FIRST,
  FirstDup = MDB_FIRST_DUP,
  GetBoth = MDB_GET_BOTH,
  GetBothRange = MDB_GET_BOTH_RANGE,
  GetCurrent = MDB_GET_CURRENT,
  GetMultiple = MDB_GET_MULTIPLE,
  Last = MDB_LAST,
  LastDup = MDB_LAST_DUP,
  Next = MDB_NEXT,
  NextDup = MDB_NEXT_DUP,
  NextMultiple = MDB_NEXT_MULTIPLE,
  NextNodup = MDB_NEXT_NODUP,
  Prev = MDB_PREV,
  PrevDup = MDB_PREV_DUP,
  PrevNodup = MDB_PREV_NODUP,
  Set = MDB_SET,
  SetKey = MDB_SET_KEY,
  SetRange = MDB_SET_RANGE,
  PrevMultiple = MDB_PREV_MULTIPLE
};

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

  std::error_code open(llvm::StringRef Path, unsigned int Flags,
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

  static llvm::ErrorOr<DBI> open(Txn &Tx, llvm::StringRef Name,
                                 unsigned int Flags = DefaultFlags);
  llvm::ErrorOr<MDB_stat> stat(Txn &Tx) const;
  llvm::ErrorOr<unsigned int> flags(Txn &Tx) const;
  std::error_code setCompare(Txn &Tx, ::MDB_cmp_func *Cmp = nullptr);
  std::error_code get(Txn &Tx, llvm::StringRef Key, llvm::StringRef &Data);
  std::error_code put(Txn &Tx, llvm::StringRef Key, llvm::StringRef &Data,
                      unsigned int Flags = 0);
  std::error_code del(Txn &Tx, llvm::StringRef Key, llvm::StringRef Data);
  std::error_code del(Txn &Tx, llvm::StringRef Key);
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
  void close() noexcept;

  std::error_code renew(Txn &Tx);
  MDB_txn *txn() const;
  MDB_dbi dbi() const;

  std::error_code get(llvm::StringRef &Key, llvm::StringRef &Data,
                      DBOperation Operation);
  std::error_code get(llvm::StringRef &Key, DBOperation Operation);
  std::error_code put(llvm::StringRef Key, llvm::StringRef &Data,
                      unsigned int Flag = 0);

  std::error_code del(unsigned int Flags = 0);
  llvm::ErrorOr<::mdb_size_t> count();
};

#define CURSOR_FOREACH_ID_QUALIFY(x) (x##__LINE__)
#define CURSOR_FOREACH_FIXUP_ERR(_Err)                                         \
  ((_Err) == lmdb::DBError::Notfound ? ((_Err) = lmdb::DBError::Success)       \
                                     : (_Err))
#define CURSOR_FOREACH(_Cursor, _Key, _Data, _Err)                             \
  bool CURSOR_FOREACH_ID_QUALIFY(_foreach_end) = false;                        \
  for ((_Err) = (_Cursor).get(_Key, _Data, lmdb::DBOperation::First),          \
      CURSOR_FOREACH_ID_QUALIFY(_foreach_end) = !!(_Err),                      \
      CURSOR_FOREACH_FIXUP_ERR(_Err);                                          \
       !(CURSOR_FOREACH_ID_QUALIFY(_foreach_end));                             \
       (_Err) = (_Cursor).get(_Key, _Data, lmdb::DBOperation::Next),           \
      CURSOR_FOREACH_ID_QUALIFY(_foreach_end) = !!(_Err),                      \
      CURSOR_FOREACH_FIXUP_ERR(_Err))
#define CURSOR_FOREACH_DUP(_Cursor, _Key, _Data, _Err)                         \
  bool CURSOR_FOREACH_ID_QUALIFY(_foreach_end) = false;                        \
  for ((_Err) = (_Cursor).get(_Key, _Data, lmdb::DBOperation::FirstDup),       \
      CURSOR_FOREACH_ID_QUALIFY(_foreach_end) = !!(_Err),                      \
      CURSOR_FOREACH_FIXUP_ERR(_Err);                                          \
       !(CURSOR_FOREACH_ID_QUALIFY(_foreach_end));                             \
       (_Err) = (_Cursor).get(_Key, _Data, lmdb::DBOperation::NextDup),        \
      CURSOR_FOREACH_ID_QUALIFY(_foreach_end) = !!(_Err),                      \
      CURSOR_FOREACH_FIXUP_ERR(_Err))

} // namespace lmdb

#endif