//===--------------- LmdbCxx.h - C++ wrapper to LMDB ------------- C++-*-===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_LMDBCXX_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_LMDBCXX_H

#include "lmdb.h"
#include "llvm/Support/Error.h"

namespace lmdb {

const char *version(int *Major, int *Minor, int *Patch);

class Error : public llvm::ErrorInfo<Error> {
public:
  static char ID;

  Error(int ErrorVal) : ErrorVal(ErrorVal) {}

  void log(llvm::raw_ostream &OS) const override { OS << message(); }
  virtual std::string message() const override {
    return mdb_strerror(ErrorVal);
  }
  std::error_code convertToErrorCode() const override {
    return llvm::inconvertibleErrorCode();
  }

  int errorValue() const { return ErrorVal; }

private:
  int ErrorVal;
};

// For get/put/del results
class Result {
public:
  enum Condition : int {
    Hit = 0,
    Miss = 1,
    Thrown = 2,
  };

  Result(int RC) : Status(Condition::Hit) {
    (void)!Status;
    switch (RC) {
    case 0:
      Status = Condition::Hit;
      break;
    case MDB_NOTFOUND:
      LLVM_FALLTHROUGH;
    case MDB_KEYEXIST:
      Status = Condition::Miss;
      break;
    default:
      Status = llvm::make_error<Error>(RC);
    }
  }

  Result(const Result &) = delete;

  Result(Result &&R) : Status(std::move(R.Status)) {}

  ~Result() {}

  Result &operator=(Result &&R) {
    moveAssign(std::move(R));
    return *this;
  }
  Result &operator=(const Result &R) = delete;

  //
  // Error checkings
  //

  bool hasError() { return !Status.operator bool(); }

  Condition condition() {
    if (hasError())
      return Condition::Thrown;
    return *Status;
  }

  llvm::Error takeError() { return Status.takeError(); }

private:
  llvm::Expected<Condition> Status;

  void moveAssign(Result &&R) {
    this->~Result();
    Status = std::move(R.Status);
  }
};

template <typename T, typename B> struct HandleWrapper {
  B getHandle() { return static_cast<T>(*this); }
};

/// This class wraps an MDB_env
class Env : public HandleWrapper<Env, MDB_env *> {
  // Default unix mode specified when opening the database
  static constexpr mdb_mode_t DefaultMode = 0666;

public:
  // Constructor
  Env() = default;

  // Constructor
  Env(Env &&E) { std::swap(Handle, E.Handle); }

  // Destructor
  virtual ~Env();

  // Expose underlying handle
  operator MDB_env *() const { return Handle; }

  // Move assignment
  Env &operator=(Env &&From) {
    if (&From == this)
      return *this;
    this->~Env();
    std::swap(Handle, From.Handle);
    return *this;
  }

  // Create an MDB_env
  static llvm::Expected<Env> create();

  // Set the size of the memory map to use for this environment
  llvm::Error setMapsize(size_t Size);

  // Set maximum number of named databases for this environment
  llvm::Error setMaxDBs(MDB_dbi N);

  // Set the maximum number of readers for this environment
  llvm::Error setMaxReaders(unsigned int Readers);

  // Set the maximum number of readers for this environment
  llvm::Error getMaxReaders(unsigned int &Readers);

  // Get the maximum key size and DUPSORT data for this environment
  int getMaxKeysize();

  // Get user context of this environment
  void *getUserctx();

  // Set user context of this environment
  llvm::Error setUserctx(void *Userctx);

  // Set assert() callback
  llvm::Error setAssert(MDB_assert_func *Func);

  // Open the environment
  llvm::Error open(llvm::StringRef Path, unsigned int Flags,
                   mdb_mode_t Mode = DefaultMode);

  // Get envinfo
  llvm::Expected<MDB_envinfo> envInfo();

private:
  // Constructor
  Env(MDB_env *P) { Handle = P; }

  // Handle to the environment
  MDB_env *Handle = nullptr;
};

/// This class wraps an MDB_txn
class Txn : public HandleWrapper<Txn, MDB_txn *> {
public:
  // Constructor
  Txn() = default;

  // Constructor
  Txn(Txn &&T) { std::swap(Handle, T.Handle); }

  // Destructor
  ~Txn();

  // Expose underlying handle
  operator MDB_txn *() const { return Handle; }

  // Move assignment
  Txn &operator=(Txn &&From) {
    if (&From == this)
      return *this;
    this->~Txn();
    std::swap(Handle, From.Handle);
    return *this;
  }

  // Begin an LMDB transaction
  static llvm::Expected<Txn> begin(MDB_env *Env, MDB_txn *ParentTxn = nullptr,
                                   unsigned int Flags = 0);

  // Return the MDB_env of this transaction
  MDB_env *env();

  // Return the transaction ID of this transaction
  size_t tid();

  // Commit
  llvm::Error commit();

  // Abort
  void abort();

private:
  // Constructor
  Txn(MDB_txn *P) : Handle(P) {}

  // Handle to the transaction
  MDB_txn *Handle = nullptr;
};

/// This class wraps an MDB_val
class Val {
public:
  // Constructor
  Val() {
    Value.mv_data = nullptr;
    Value.mv_size = 0;
  }

  // Constructor
  Val(const Val &V) : Value(V.Value) {}

  // Constructor
  Val(Val &&) = default;

  // Constructor
  Val(const MDB_val &V) : Value(V) {}

  // Constructor
  Val(const std::string &S) {
    Value.mv_data = const_cast<char *>(S.data());
    Value.mv_size = S.size();
  }

  // Constructor
  Val(llvm::StringRef S) {
    Value.mv_data = const_cast<char *>(S.data());
    Value.mv_size = S.size();
  }

  // Constructor
  Val(llvm::ArrayRef<uint8_t> A) {
    Value.mv_data = const_cast<uint8_t *>(A.data());
    Value.mv_size = A.size();
  }

  // Constructor
  Val(const void *P, size_t S) { assign(P, S); }

  // Constructor
  template <typename T> Val(const T *S) {
    Value.mv_data = const_cast<T *>(S);
    Value.mv_size = sizeof(T);
  }

  // Move assignment
  Val &operator=(Val &&) = default;

  // Assign
  void assign(const void *P, size_t S) {
    Value.mv_data = const_cast<void *>(P);
    Value.mv_size = S;
  }

  // To MDB_val
  operator MDB_val() const { return Value; }

  // To MDB_val *
  operator MDB_val *() { return &Value; }

  // To llvm::StringRef
  operator llvm::StringRef() const {
    return llvm::StringRef(reinterpret_cast<char *>(Value.mv_data),
                           Value.mv_size);
  }

  // To llvm::ArrayRef<uint8_t>
  operator llvm::ArrayRef<uint8_t>() const {
    return llvm::ArrayRef<uint8_t>(reinterpret_cast<uint8_t *>(Value.mv_data),
                                   Value.mv_size);
  }

  // To llvm::MutableArrayRef<uint8_t>
  operator llvm::MutableArrayRef<uint8_t>() {
    return llvm::MutableArrayRef<uint8_t>(
        reinterpret_cast<uint8_t *>(Value.mv_data), Value.mv_size);
  }

  // Get data pointer
  char *data() { return reinterpret_cast<char *>(Value.mv_data); }

  // Get data pointer
  const char *data() const { return reinterpret_cast<char *>(Value.mv_data); }

  // Get data pointer
  template <typename T> T *data() {
    return reinterpret_cast<T *>(Value.mv_data);
  }

  // Get data pointer
  template <typename T> const T *data() const {
    return reinterpret_cast<T *>(Value.mv_data);
  }

  // Get size
  size_t size() const { return Value.mv_size; }

private:
  MDB_val Value;
};

/// This class wraps an MDB_dbi
class DBI : public HandleWrapper<DBI, std::pair<MDB_env *, MDB_dbi>> {
  static constexpr MDB_dbi InvalidDBI = -1u;

public:
  // Constructor
  DBI() : Handle(nullptr, InvalidDBI){};

  // Constructor
  DBI(DBI &&T) : DBI() { std::swap(Handle, T.Handle); }

  // Destructor
  ~DBI();

  // Expose underlying handle
  operator std::pair<MDB_env *, MDB_dbi>() const { return Handle; }

  // Expose underlying environment handle
  operator MDB_env *() const { return Handle.first; }

  // Expose underlying dbi handle
  operator MDB_dbi() const { return Handle.second; }

  // Move assignment
  DBI &operator=(DBI &&From) {
    if (&From == this)
      return *this;
    this->~DBI();
    std::swap(Handle, From.Handle);
    return *this;
  }

  // Open the database
  static llvm::Expected<DBI> open(MDB_txn *Txn, const char *Name,
                                  unsigned int Flags);

  // Drop the database
  llvm::Error drop(MDB_txn *Txn, int Del = 0);

  // Get key-value pair from database.
  // Return true for found key, false for missing key.
  Result get(MDB_txn *Txn, const Val &Key, Val &Data);

  // Put key-value pair to database
  Result put(MDB_txn *Txn, const Val &Key, Val &Data, unsigned int Flags);
  // Put key-value pair to database
  Result put(MDB_txn *Txn, const Val &Key, Val &&Data, unsigned int Flags);

  // Delete key-value pair to database
  Result del(MDB_txn *Txn, const Val &Key, llvm::Optional<Val> Data);

  // Get statistics of database
  llvm::Expected<MDB_stat> stat(MDB_txn *Txn);

private:
  // Constructor
  DBI(MDB_env *E, MDB_dbi I) : Handle(E, I){};

  // Handle to the database
  std::pair<MDB_env *, MDB_dbi> Handle;
};

/// This class wraps an MDB_cursor
class Cursor : public HandleWrapper<Cursor, MDB_cursor *> {
public:
  // Constructor
  Cursor() = default;

  // Constructor
  Cursor(Cursor &&C) : Handle(nullptr) { std::swap(Handle, C.Handle); }

  // Destructor
  ~Cursor();

  // Expose underlying handle
  operator MDB_cursor *() const { return Handle; }

  // Move assignment
  Cursor &operator=(Cursor &&From) {
    if (&From == this)
      return *this;
    this->~Cursor();
    std::swap(Handle, From.Handle);
    return *this;
  }

  // Open a cursor
  static llvm::Expected<Cursor> open(MDB_txn *Txn, MDB_dbi DBI);

  // Get operation for cursor with null data argument
  Result get(Val &Key, MDB_cursor_op Op);

  // Get pointer to data at certain position
  Result get(Val &Key, Val &Data, MDB_cursor_op Op);

  // Put data
  Result put(const Val &Key, Val &Data, unsigned int Flags);
  Result put(const Val &Key, Val &&Data, unsigned int Flags);

  // Put key and reserve space for users to put data later
  Result reserve(const Val &Key, Val &Data, unsigned int Flags);

  // Delete data pointed by the cursor
  Result del(unsigned int Flags = 0);

  // Return the number of entries for current key
  llvm::Expected<size_t> count();

private:
  // Constructor
  Cursor(MDB_cursor *C) : Handle(C){};

  // Handle to the cursor
  MDB_cursor *Handle = nullptr;
};

} // namespace lmdb

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DBINDEX_LMDBCXX_H
