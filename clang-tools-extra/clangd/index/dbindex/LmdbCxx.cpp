//===--------------- LmdbCxx.cpp - C++ wrapper to LMDB ------------- C++-*-===//

#include "LmdbCxx.h"
#include "lmdb.h"

namespace lmdb {

ErrorCategory ErrCat;

const char *version(int *Major, int *Minor, int *Patch) {
  return mdb_version(Major, Minor, Patch);
}

Env::~Env() {
  if (Handle)
    mdb_env_close(Handle);
}

llvm::ErrorOr<Env> Env::create() {
  MDB_env *Handle;
  int RC = mdb_env_create(&Handle);
  if (RC != MDB_SUCCESS)
    return makeErrorCode(RC);
  return Env(Handle);
}

std::error_code Env::setMapsize(size_t Size) {
  int RC = mdb_env_set_mapsize(Handle, Size);
  return makeErrorCode(RC);
}

std::error_code Env::setMaxDBs(MDB_dbi N) {
  int RC = mdb_env_set_maxdbs(Handle, N);
  return makeErrorCode(RC);
}

std::error_code Env::setMaxReaders(unsigned int Readers) {
  int RC = mdb_env_set_maxreaders(Handle, Readers);
  return makeErrorCode(RC);
}

std::error_code Env::getMaxReaders(unsigned int &Readers) {
  int RC = mdb_env_get_maxreaders(Handle, &Readers);
  return makeErrorCode(RC);
}

int Env::getMaxKeysize() { return mdb_env_get_maxkeysize(Handle); }

void *Env::getUserctx() { return mdb_env_get_userctx(Handle); }

std::error_code Env::setUserctx(void *Userctx) {
  int RC = mdb_env_set_userctx(Handle, Userctx);
  return makeErrorCode(RC);
}

std::error_code Env::setAssert(MDB_assert_func *Func) {
  int RC = mdb_env_set_assert(Handle, Func);
  return makeErrorCode(RC);
}

std::error_code Env::open(llvm::StringRef Path, unsigned int Flags,
                          mdb_mode_t Mode) {
  int RC = mdb_env_open(Handle, Path.data(), Flags, Mode);
  return makeErrorCode(RC);
}

std::error_code Env::envInfo(MDB_envinfo &EnvInfo) {
  int RC = mdb_env_info(Handle, &EnvInfo);
  return makeErrorCode(RC);
}

Txn::~Txn() {
  if (Handle)
    mdb_txn_abort(Handle);
}

llvm::ErrorOr<Txn> Txn::begin(MDB_env *Env, MDB_txn *ParentTxn,
                              unsigned int Flags) {
  MDB_txn *Handle;
  int RC = mdb_txn_begin(Env, ParentTxn, Flags, &Handle);
  if (RC != MDB_SUCCESS)
    return makeErrorCode(RC);
  return Txn(Handle);
}

MDB_env *Txn::env() { return mdb_txn_env(Handle); }

size_t Txn::tid() { return mdb_txn_id(Handle); }

std::error_code Txn::commit() {
  int RC = mdb_txn_commit(Handle);
  Handle = nullptr;
  return makeErrorCode(RC);
}

void Txn::abort() {
  mdb_txn_abort(Handle);
  Handle = nullptr;
}

DBI::~DBI() {
  if (Handle.first != nullptr)
    mdb_dbi_close(Handle.first, Handle.second);
}

llvm::ErrorOr<DBI> DBI::open(MDB_txn *Txn, const char *Name,
                             unsigned int Flags) {
  MDB_dbi Handle;
  int RC = mdb_dbi_open(Txn, Name, Flags, &Handle);
  if (RC != MDB_SUCCESS)
    return makeErrorCode(RC);
  return DBI(mdb_txn_env(Txn), Handle);
}

std::error_code DBI::drop(MDB_txn *Txn, int Del) {
  int RC = mdb_drop(Txn, Handle.second, Del);
  return makeErrorCode(RC);
}

std::error_code DBI::get(MDB_txn *Txn, const Val &Key, Val &Data) {
  MDB_val KV = Key;
  int RC = mdb_get(Txn, Handle.second, &KV, Data);
  return makeErrorCode(RC);
}

std::error_code DBI::put(MDB_txn *Txn, const Val &Key, Val &Data,
                         unsigned int Flags) {
  MDB_val KV = Key;
  int RC = mdb_put(Txn, Handle.second, &KV, Data, Flags);
  return makeErrorCode(RC);
}

std::error_code DBI::put(MDB_txn *Txn, const Val &Key, Val &&Data,
                         unsigned int Flags) {
  return put(Txn, Key, Data, Flags);
}

std::error_code DBI::del(MDB_txn *Txn, const Val &Key,
                         llvm::Optional<Val> Data) {
  MDB_val KV = Key;
  int RC = mdb_del(Txn, Handle.second, &KV,
                   Data ? static_cast<MDB_val *>(*Data) : nullptr);
  return makeErrorCode(RC);
}

std::error_code DBI::stat(MDB_txn *Txn, MDB_stat &Stat) {
  int RC = mdb_stat(Txn, Handle.second, &Stat);
  return makeErrorCode(RC);
}

Cursor::~Cursor() {
  if (Handle)
    mdb_cursor_close(Handle);
}

llvm::ErrorOr<Cursor> Cursor::open(MDB_txn *Txn, MDB_dbi DBI) {
  MDB_cursor *Handle;
  int RC = mdb_cursor_open(Txn, DBI, &Handle);
  if (RC != MDB_SUCCESS)
    return makeErrorCode(RC);
  return Cursor(Handle);
}

std::error_code Cursor::get(Val &Key, MDB_cursor_op Op) {
  int RC = mdb_cursor_get(Handle, Key, nullptr, Op);
  return makeErrorCode(RC);
}

std::error_code Cursor::get(Val &Key, Val &Data, MDB_cursor_op Op) {
  int RC = mdb_cursor_get(Handle, Key, Data, Op);
  return makeErrorCode(RC);
}

std::error_code Cursor::put(const Val &Key, Val &Data, unsigned int Flags) {
  MDB_val KV = Key;
  int RC = mdb_cursor_put(Handle, &KV, Data, Flags);
  return makeErrorCode(RC);
}

std::error_code Cursor::put(const Val &Key, Val &&Data, unsigned int Flags) {
  return put(Key, Data, Flags);
}

std::error_code Cursor::del(unsigned int Flags) {
  int RC = mdb_cursor_del(Handle, Flags);
  return makeErrorCode(RC);
}

llvm::ErrorOr<size_t> Cursor::count() {
  size_t Size;
  int RC = mdb_cursor_count(Handle, &Size);
  if (RC != MDB_SUCCESS)
    return makeErrorCode(RC);
  return Size;
}

} // namespace lmdb
