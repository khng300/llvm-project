//===--- LMDBWrapper.cpp - LMDB Wrapper -------------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "LMDBWrapper.h"
#include "llvm/Support/ErrorHandling.h"

namespace lmdb {
namespace {
DBErrorCategory DBErrCategory;
} // namespace

const char *DBErrorCategory::name() const noexcept {
  return "lmdbwrapper.error";
}

std::string DBErrorCategory::message(int Condition) const {
  return ::mdb_strerror(Condition);
}

std::error_code make_error_code(lmdb::DBError E) {
  return std::error_code(static_cast<int>(E), DBErrCategory);
}

void Environment::moveAssign(Environment &&Env) noexcept {
  if (&Env == this)
    return;
  close();
  std::swap(Env.Handle, Handle);
}

llvm::ErrorOr<Environment> Environment::create(unsigned int Flags) {
  MDB_env *Handle;
  int EV = ::mdb_env_create(&Handle);
  if (EV != 0)
    return static_cast<DBError>(EV);
  auto Env = Environment(Handle);
  EV = ::mdb_env_set_flags(Env.Handle, Flags, 1);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Env;
}

void Environment::close() noexcept {
  if (Handle) {
    ::mdb_env_close(Handle);
    Handle = nullptr;
  }
}

std::error_code Environment::setFlags(unsigned int Flags, bool Set) {
  int EV = ::mdb_env_set_flags(Handle, Flags, Set ? 1 : 0);
  return static_cast<DBError>(EV);
}

std::error_code Environment::setMapSize(std::size_t MapSize) {
  int EV = ::mdb_env_set_mapsize(Handle, MapSize);
  return static_cast<DBError>(EV);
}

std::error_code Environment::setMaxDBs(MDB_dbi Count) {
  int EV = ::mdb_env_set_maxdbs(Handle, Count);
  return static_cast<DBError>(EV);
}

std::error_code Environment::setMaxReaders(unsigned int Count) {
  int EV = ::mdb_env_set_maxreaders(Handle, Count);
  return static_cast<DBError>(EV);
}

::MDB_envinfo Environment::getEnvInfo() {
  ::MDB_envinfo Ret;
  if (::mdb_env_info(Handle, &Ret) != 0)
    llvm::report_fatal_error("mdb_env_info bad arg!");
  return Ret;
}

std::error_code Environment::open(std::string_view Path, unsigned int Flags,
                                  ::mdb_mode_t Mode) {
  int EV = ::mdb_env_open(Handle, Path.data(), Flags, Mode);
  return static_cast<DBError>(EV);
}

void Txn::close() noexcept {
  if (Handle) {
    ::mdb_txn_abort(Handle);
    Handle = nullptr;
  }
}

void Txn::moveAssign(Txn &&Tx) noexcept {
  if (&Tx == this)
    return;
  close();
  std::swap(Tx.Handle, Handle);
}

llvm::ErrorOr<Txn> Txn::begin(Environment &Env, unsigned int Flags) {
  MDB_txn *Handle;
  int EV = ::mdb_txn_begin(Env.handle({}), nullptr, Flags, &Handle);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Txn(Handle, Flags);
}

void Txn::reset() { ::mdb_txn_reset(Handle); }

mdb_size_t Txn::txnID() { return mdb_txn_id(Handle); }

std::error_code Txn::commit() {
  if (Handle != nullptr) {
    int EV = ::mdb_txn_commit(Handle);
    Handle = nullptr;
    return static_cast<DBError>(EV);
  }
  return static_cast<DBError>(EINVAL);
}

void Txn::abort() noexcept { close(); }

llvm::ErrorOr<DBI> DBI::open(Txn &Tx, std::string_view Name,
                             unsigned int Flags) {
  MDB_dbi Handle;
  int EV = ::mdb_dbi_open(Tx.handle({}), Name.data(), Flags, &Handle);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return DBI(Handle);
}

llvm::ErrorOr<MDB_stat> DBI::stat(Txn &Tx) const {
  MDB_stat Result;
  int EV = ::mdb_stat(Tx.handle({}), Handle, &Result);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Result;
}

llvm::ErrorOr<unsigned int> DBI::flags(Txn &Tx) const {
  unsigned int Result;
  int EV = ::mdb_dbi_flags(Tx.handle({}), Handle, &Result);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Result;
}

std::error_code DBI::setCompare(Txn &Tx, ::MDB_cmp_func *Cmp) {
  int EV = ::mdb_set_compare(Tx.handle({}), Handle, Cmp);
  return static_cast<DBError>(EV);
}

std::error_code DBI::get(Txn &Tx, Slice Key, Slice &Data) {
  ::MDB_val KV = Key, DV = Data;
  int EV = ::mdb_get(Tx.handle({}), Handle, &KV, &DV);
  if (EV != 0)
    return static_cast<DBError>(EV);
  Data = DV;
  return {};
}

std::error_code DBI::put(Txn &Tx, Slice Key, Slice &Data, unsigned int Flags) {
  ::MDB_val KV = Key, DV = Data;
  int EV = ::mdb_put(Tx.handle({}), Handle, &KV, &DV, Flags);
  if (EV != 0 && EV != MDB_KEYEXIST)
    return static_cast<DBError>(EV);
  Data = DV;
  return static_cast<DBError>(EV);
}

std::error_code DBI::del(Txn &Tx, Slice Key, Slice Data) {
  ::MDB_val KV = Key, DV = Data;
  int EV = ::mdb_del(Tx.handle({}), Handle, &KV, &DV);
  return static_cast<DBError>(EV);
}

std::error_code DBI::del(Txn &Tx, Slice Key) {
  ::MDB_val KV = Key;
  int EV = ::mdb_del(Tx.handle({}), Handle, &KV, NULL);
  return static_cast<DBError>(EV);
}

std::error_code DBI::drop(Txn &Tx, const bool Del) {
  int EV = ::mdb_drop(Tx.handle({}), Handle, Del ? 1 : 0);
  return static_cast<DBError>(EV);
}

llvm::ErrorOr<Cursor> Cursor::dup() {
  lmdb::Cursor Res;
  int EV = ::mdb_cursor_open(txn(), dbi(), &Res.Handle);
  if (EV != 0)
    return static_cast<DBError>(EV);
  ::MDB_val KV, DV;
  EV = ::mdb_cursor_get(Handle, &KV, &DV, MDB_GET_CURRENT);
  if (EV != 0)
    return static_cast<DBError>(EV);
  EV = ::mdb_cursor_get(Res.Handle, &KV, &DV, MDB_GET_BOTH);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Res;
}

void Cursor::moveAssign(Cursor &&Cur) noexcept {
  if (&Cur == this)
    return;
  close();
  std::swap(Cur.Handle, Handle);
}

llvm::ErrorOr<Cursor> Cursor::open(MDB_txn *Tx, MDB_dbi Dbi) {
  MDB_cursor *Handle;
  int EV = ::mdb_cursor_open(Tx, Dbi, &Handle);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Cursor(Handle);
}

void Cursor::close() noexcept {
  if (Handle) {
    ::mdb_cursor_close(Handle);
    Handle = nullptr;
  }
}

std::error_code Cursor::renew(Txn &Tx) {
  int EV = ::mdb_cursor_renew(Tx.handle({}), Handle);
  return static_cast<DBError>(EV);
}

MDB_txn *Cursor::txn() const { return ::mdb_cursor_txn(Handle); }

MDB_dbi Cursor::dbi() const { return ::mdb_cursor_dbi(Handle); }

std::error_code Cursor::get(Slice &Key, Slice &Data,
                            ::MDB_cursor_op Operation) {
  MDB_val KV = Key, DV = Data;
  int EV = ::mdb_cursor_get(Handle, &KV, &DV, Operation);
  if (EV != 0)
    return static_cast<DBError>(EV);
  Key = KV;
  Data = DV;
  return {};
}

std::error_code Cursor::get(Slice &Key, ::MDB_cursor_op Operation) {
  MDB_val KV = Key;
  int EV = ::mdb_cursor_get(Handle, &KV, NULL, Operation);
  if (EV != 0)
    return static_cast<DBError>(EV);
  Key = KV;
  return {};
}

std::error_code Cursor::put(Slice Key, Slice &Data, unsigned int Flags) {
  MDB_val KV = Key, DV = Data;
  int EV = ::mdb_cursor_put(Handle, &KV, &DV, Flags);
  if (EV != 0 && EV != MDB_KEYEXIST)
    return static_cast<DBError>(EV);
  Data = DV;
  return {};
}

std::error_code Cursor::del(unsigned int Flags) {
  int EV = ::mdb_cursor_del(Handle, Flags);
  return static_cast<DBError>(EV);
}

llvm::ErrorOr<::mdb_size_t> Cursor::count() {
  ::mdb_size_t Result;
  int EV = ::mdb_cursor_count(Handle, &Result);
  if (EV != 0)
    return static_cast<DBError>(EV);
  return Result;
}

std::optional<Slice> DupIterator::opImpl(::MDB_cursor_op Op) {
  assert(lmdb::Cursor::valid());
  assert(!ErrorCode);

  Slice Key, Data;
  auto EC = Cursor::get(Key, Data, Op);
  if (EC) {
    if (EC != lmdb::DBError::Notfound)
      ErrorCode = EC;
    return std::nullopt;
  }
  return Data;
}

llvm::ErrorOr<size_t> DupIterator::count() { return Cursor::count(); }

std::optional<Slice> DupIterator::seek(Slice Data) {
  assert(lmdb::Cursor::valid());
  assert(!ErrorCode);

  auto EC = [this, &Data]() {
    Slice Key;
    auto EC = Cursor::get(Key, MDB_GET_CURRENT);
    if (EC)
      return EC;
    return Cursor::get(Key, Data, MDB_GET_BOTH_RANGE);
  }();
  if (EC) {
    if (EC != lmdb::DBError::Notfound)
      ErrorCode = EC;
    return std::nullopt;
  }
  return Data;
}

} // namespace lmdb
