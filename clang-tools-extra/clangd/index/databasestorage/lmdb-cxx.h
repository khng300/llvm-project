//===-------------- lmdb-cxx.h - C++ wrapper to LMDB ------------- C++-*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===--------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DATABASESTORAGE_LMDB_CXX_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DATABASESTORAGE_LMDB_CXX_H

#include "lmdb/libraries/liblmdb/lmdb.h"
#include "llvm/Support/Error.h"

namespace lmdb {

const char *version(int *Major, int *Minor, int *Patch);

/// This class wraps an LMDB error
class DBError final : public llvm::ErrorInfo<DBError> {
public:
  // Used by ErrorInfo::classID.
  static char ID;

  // Construct an error with origin and return code
  DBError(const char *const Origin, int RC) : Origin(Origin), RC(RC) {}

  // Log the error to output stream
  void log(llvm::raw_ostream &OS) const override {
    OS << "'" << Origin << "': " << mdb_strerror(RC) << '\n';
  }

  // Convert the error to std::error_code
  std::error_code convertToErrorCode() const override;

  // Get message related to the error
  const char *strerror() const;

  // Get RC code
  int returnCode() const { return RC; }

private:
  const char *Origin;
  int RC;
};

template <typename T, typename B> struct HandleWrapper {
  B getHandle() { return static_cast<T>(*this); }
};

/// This class wraps an MDB_env
class Env : public HandleWrapper<Env, MDB_env *> {
  // Default unix mode specified when opening the database
  static constexpr mdb_mode_t DefaultMode = 0644;

public:
  // Constructor
  Env() = default;

  // Constructor
  Env(Env &&E) { *this = std::move(E); }

  // Destructor
  virtual ~Env();

  // Expose underlying handle
  operator MDB_env *() const { return Handle; }

  // Move assignment
  Env &operator=(Env &&E) {
    Env::~Env();
    Handle = E.Handle;
    E.Handle = nullptr;
    return *this;
  }

  // Create an MDB_env
  static llvm::Expected<Env> create();

  // Set the size of the memory map to use for this environment.
  llvm::Error setMapsize(mdb_size_t Size);

  // Set maximum number of named databases for this environment.
  llvm::Error setMaxDBs(MDB_dbi N);

  // Open the environment
  llvm::Error open(llvm::StringRef Path, unsigned int Flags,
                   mdb_mode_t Mode = DefaultMode);

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
  Txn(Txn &&T) { *this = std::move(T); }

  // Destructor
  ~Txn();

  // Expose underlying handle
  operator MDB_txn *() const { return Handle; }

  // Move assignment
  Txn &operator=(Txn &&T) {
    Txn::~Txn();
    Handle = T.Handle;
    T.Handle = nullptr;
    return *this;
  }

  // Begin an LMDB transaction
  static llvm::Expected<Txn> begin(MDB_env *Env, MDB_txn *ParentTxn = nullptr,
                                   unsigned int Flags = 0);

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
  template <typename T> Val(const T *S) {
    Value.mv_data = const_cast<T *>(S);
    Value.mv_size = sizeof(T);
  }

  // Move assignment
  Val &operator=(Val &&) = default;

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

static inline llvm::Error filterDBError(llvm::Error Err, int RC) {
  return llvm::handleErrors(
      std::move(Err),
      [&](std::unique_ptr<lmdb::DBError> ErrorInfo) -> llvm::Error {
        if (ErrorInfo->returnCode() != RC)
          return llvm::Error(std::move(ErrorInfo));
        return llvm::Error::success();
      });
}

/// This class wraps an MDB_dbi
class DBI : public HandleWrapper<DBI, std::pair<MDB_env *, MDB_dbi>> {
  static constexpr MDB_dbi InvalidDBI = -1u;

public:
  // Constructor
  DBI() : Handle(nullptr, InvalidDBI){};

  // Constructor
  DBI(DBI &&T) : DBI() { *this = std::move(T); }

  // Destructor
  ~DBI();

  // Expose underlying handle
  operator std::pair<MDB_env *, MDB_dbi>() const { return Handle; }

  // Expose underlying environment handle
  operator MDB_env *() const { return Handle.first; }

  // Expose underlying dbi handle
  operator MDB_dbi() const { return Handle.second; }

  // Move assignment
  DBI &operator=(DBI &&T) {
    DBI::~DBI();
    Handle = T.Handle;
    T.Handle = std::pair<MDB_env *, MDB_dbi>();
    return *this;
  }

  // Open the database
  static llvm::Expected<DBI> open(MDB_txn *Txn, const char *Name,
                                  unsigned int Flags);

  // Get key-value pair from database
  llvm::Error get(MDB_txn *Txn, const Val &Key, Val &Data);

  // Put key-value pair to database
  llvm::Error put(MDB_txn *Txn, const Val &Key, Val &Data, unsigned int Flags);

  // Delete key-value pair to database
  llvm::Error del(MDB_txn *Txn, const Val &Key, llvm::Optional<Val> Data);

private:
  // Constructor
  DBI(MDB_env *E, MDB_dbi I) : Handle(E, I){};

  // Handle to the database
  std::pair<MDB_env *, MDB_dbi> Handle;
};

/// This enumerates the control value for iterator
enum IteratorControl { Continue = 0, Stop = 1 };

/// This class wraps an MDB_cursor
class Cursor : public HandleWrapper<Cursor, MDB_cursor *> {
public:
  // Constructor
  Cursor() = default;

  // Constructor
  Cursor(Cursor &&C) : Handle(nullptr) { *this = std::move(C); }

  // Destructor
  ~Cursor();

  // Expose underlying handle
  operator MDB_cursor *() const { return Handle; }

  // Move assignment
  Cursor &operator=(Cursor &&C) {
    Cursor::~Cursor();
    Handle = C.Handle;
    C.Handle = nullptr;
    return C;
  }

  // Open a cursor
  static llvm::Expected<Cursor> open(MDB_txn *Txn, MDB_dbi DBI);

  // Set the cursor to certain position
  llvm::Error set(const Val &Key);

  // Set the cursor to certain position and get the pointer to
  // the key
  llvm::Error setKey(Val &Key);

  // Set the cursor to certain position with key greater than
  // or equal to specified key
  llvm::Error setRange(Val &Key);

  // Get pointer to data at certain position
  llvm::Error get(Val &Key, Val &Data, MDB_cursor_op Op);

  // Put data
  llvm::Error put(const Val &Key, const Val &Data, unsigned int Flags);

  // Put key and reserve space for users to put data later
  llvm::Error reserve(const Val &Key, Val &Data, unsigned int Flags);

  // Delete data pointed by the cursor
  llvm::Error del(unsigned int Flags = 0);

  // Ranged foreach
  llvm::Error foreachKey(
      const Val &Key,
      llvm::function_ref<IteratorControl(const Val &, const Val &)> Callback);

  // Ranged foreach
  llvm::Error foreachRanged(
      const Val &Prefix,
      llvm::function_ref<IteratorControl(const Val &, const Val &)> Callback);

private:
  // Constructor
  Cursor(MDB_cursor *C) : Handle(C){};

  // Handle to the cursor
  MDB_cursor *Handle = nullptr;
};

} // namespace lmdb

#endif // LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DATABASESTORAGE_LMDB_CXX_H