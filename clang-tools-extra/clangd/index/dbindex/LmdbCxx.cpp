//===--------------- LmdbCxx.cpp - C++ wrapper to LMDB ------------- C++-*-===//

#include "LmdbCxx.h"
#include "lmdb.h"

namespace lmdb {

char Error::ID;

const char *version(int *Major, int *Minor, int *Patch) {
  return mdb_version(Major, Minor, Patch);
}

Env::~Env() {
  if (Handle) {
    mdb_env_close(Handle);
    Handle = nullptr;
  }
}

llvm::Expected<Env> Env::create() {
  MDB_env *Handle;
  int RC = mdb_env_create(&Handle);
  if (RC)
    return llvm::make_error<Error>(RC);
  return Env(Handle);
}

llvm::Error Env::setMapsize(size_t Size) {
  int RC = mdb_env_set_mapsize(Handle, Size);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

llvm::Error Env::setMaxDBs(MDB_dbi N) {
  int RC = mdb_env_set_maxdbs(Handle, N);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

llvm::Error Env::setMaxReaders(unsigned int Readers) {
  int RC = mdb_env_set_maxreaders(Handle, Readers);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

llvm::Error Env::getMaxReaders(unsigned int &Readers) {
  int RC = mdb_env_get_maxreaders(Handle, &Readers);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

int Env::getMaxKeysize() { return mdb_env_get_maxkeysize(Handle); }

void *Env::getUserctx() { return mdb_env_get_userctx(Handle); }

llvm::Error Env::setUserctx(void *Userctx) {
  int RC = mdb_env_set_userctx(Handle, Userctx);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

llvm::Error Env::setAssert(MDB_assert_func *Func) {
  int RC = mdb_env_set_assert(Handle, Func);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

llvm::Error Env::open(llvm::StringRef Path, unsigned int Flags,
                      mdb_mode_t Mode) {
  int RC = mdb_env_open(Handle, Path.data(), Flags, Mode);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

llvm::Expected<MDB_envinfo> Env::envInfo() {
  MDB_envinfo EnvInfo;
  int RC = mdb_env_info(Handle, &EnvInfo);
  if (RC)
    return llvm::make_error<Error>(RC);
  return EnvInfo;
}

Txn::~Txn() {
  if (Handle) {
    mdb_txn_abort(Handle);
    Handle = nullptr;
  }
}

llvm::Expected<Txn> Txn::begin(MDB_env *Env, MDB_txn *ParentTxn,
                               unsigned int Flags) {
  MDB_txn *Handle;
  int RC = mdb_txn_begin(Env, ParentTxn, Flags, &Handle);
  if (RC)
    return llvm::make_error<Error>(RC);
  return Txn(Handle);
}

MDB_env *Txn::env() { return mdb_txn_env(Handle); }

size_t Txn::tid() { return mdb_txn_id(Handle); }

llvm::Error Txn::commit() {
  int RC = mdb_txn_commit(Handle);
  Handle = nullptr;
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

void Txn::abort() {
  mdb_txn_abort(Handle);
  Handle = nullptr;
}

DBI::~DBI() {
  if (Handle.first != nullptr) {
    mdb_dbi_close(Handle.first, Handle.second);
    Handle = std::pair<MDB_env *, MDB_dbi>();
  }
}

llvm::Expected<DBI> DBI::open(MDB_txn *Txn, const char *Name,
                              unsigned int Flags) {
  MDB_dbi Handle;
  int RC = mdb_dbi_open(Txn, Name, Flags, &Handle);
  if (RC)
    return llvm::make_error<Error>(RC);
  return DBI(mdb_txn_env(Txn), Handle);
}

llvm::Error DBI::drop(MDB_txn *Txn, int Del) {
  int RC = mdb_drop(Txn, Handle.second, Del);
  if (RC)
    return llvm::make_error<Error>(RC);
  return llvm::Error::success();
}

Result DBI::get(MDB_txn *Txn, const Val &Key, Val &Data) {
  MDB_val KV = Key;
  int RC = mdb_get(Txn, Handle.second, &KV, Data);
  return Result(RC);
}

Result DBI::put(MDB_txn *Txn, const Val &Key, Val &Data, unsigned int Flags) {
  MDB_val KV = Key;
  int RC = mdb_put(Txn, Handle.second, &KV, Data, Flags);
  return Result(RC);
}

Result DBI::put(MDB_txn *Txn, const Val &Key, Val &&Data, unsigned int Flags) {
  return put(Txn, Key, Data, Flags);
}

Result DBI::del(MDB_txn *Txn, const Val &Key, llvm::Optional<Val> Data) {
  MDB_val KV = Key;
  int RC = mdb_del(Txn, Handle.second, &KV,
                   Data ? static_cast<MDB_val *>(*Data) : nullptr);
  return Result(RC);
}

llvm::Expected<MDB_stat> DBI::stat(MDB_txn *Txn) {
  MDB_stat Stat;
  int RC = mdb_stat(Txn, Handle.second, &Stat);
  if (RC)
    return llvm::make_error<Error>(RC);
  return Stat;
}

Cursor::~Cursor() {
  if (Handle) {
    mdb_cursor_close(Handle);
    Handle = nullptr;
  }
}

llvm::Expected<Cursor> Cursor::open(MDB_txn *Txn, MDB_dbi DBI) {
  MDB_cursor *Handle;
  int RC = mdb_cursor_open(Txn, DBI, &Handle);
  if (RC)
    return llvm::make_error<Error>(RC);
  return Cursor(Handle);
}

Result Cursor::get(Val &Key, MDB_cursor_op Op) {
  int RC = mdb_cursor_get(Handle, Key, nullptr, Op);
  return Result(RC);
}

Result Cursor::get(Val &Key, Val &Data, MDB_cursor_op Op) {
  int RC = mdb_cursor_get(Handle, Key, Data, Op);
  return Result(RC);
}

Result Cursor::put(const Val &Key, Val &Data, unsigned int Flags) {
  MDB_val KV = Key;
  int RC = mdb_cursor_put(Handle, &KV, Data, Flags);
  return Result(RC);
}

Result Cursor::put(const Val &Key, Val &&Data, unsigned int Flags) {
  return put(Key, Data, Flags);
}

Result Cursor::del(unsigned int Flags) {
  int RC = mdb_cursor_del(Handle, Flags);
  return Result(RC);
}

llvm::Expected<size_t> Cursor::count() {
  size_t Size;
  int RC = mdb_cursor_count(Handle, &Size);
  if (RC)
    return llvm::make_error<Error>(RC);
  return Size;
}

} // namespace lmdb
