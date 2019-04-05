//===--- DatabaseStorage.cpp - Dynamic on-disk symbol index. ------C++-*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===--------------------------------------------------------------------===//

#include "DatabaseStorage.h"
#include "Logger.h"
#include "URI.h"
#include "index/Merge.h"
#include "index/Serialization.h"
#include "lmdb-cxx.h"
#include "llvm/ADT/ScopeExit.h"
#include "llvm/ADT/StringExtras.h"
#include "llvm/Support/BinaryByteStream.h"
#include "llvm/Support/BinaryStreamReader.h"
#include "llvm/Support/BinaryStreamWriter.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/SHA1.h"

namespace clang {
namespace clangd {

static constexpr llvm::support::endianness SerializeEndianness =
    llvm::support::endianness::big;

using HashedID = std::array<uint8_t, 8>;

class LMDBStorage : public DatabaseStorage {
public:
  static const char *FILEURI_SYMBOLS_COLUMN_NAME;
  static const char *FILEURI_REFS_COLUMN_NAME;
  static const char *SYMBOLS_COLUMN_NAME;
  static const char *REFS_COLUMN_NAME;

  static std::unique_ptr<LMDBStorage> open(PathRef DBPath);

  ~LMDBStorage() override { mdb_env_close(DBEnv); };

  void update(PathRef File, std::unique_ptr<SymbolSlab> Syms,
              std::unique_ptr<RefSlab> Refs) override;

  bool
  fuzzyFind(const FuzzyFindRequest &Req,
            llvm::function_ref<void(const Symbol &)> Callback) const override {
    return false;
  }

  void lookup(const LookupRequest &Req,
              llvm::function_ref<void(const Symbol &)> Callback) const override;

  void refs(const RefsRequest &Req,
            llvm::function_ref<void(const Ref &)> Callback) const override;

private:
  llvm::Error addFile(lmdb::Txn &Txn, llvm::StringRef FilePath,
                      const SymbolSlab &Syms, const RefSlab &Refs);

  llvm::Error removeFile(lmdb::Txn &Txn, llvm::StringRef FilePath);

  llvm::Error addSymbol(lmdb::Txn &Txn, llvm::StringRef FilePath,
                        const SymbolSlab &Syms);

  llvm::Error removeSymbol(lmdb::Txn &Txn, llvm::StringRef FilePath,
                           const std::vector<SymbolID> &IDs);

  llvm::Error addRef(lmdb::Txn &Txn, llvm::StringRef FilePath,
                     const RefSlab &Refs);

  llvm::Error removeRefs(lmdb::Txn &Txn, llvm::StringRef FilePath, SymbolID ID,
                         llvm::ArrayRef<Ref> Refs);

  llvm::Error
  getSymbol(lmdb::Txn &Txn, SymbolID ID,
            llvm::function_ref<void(const Symbol &)> Callback) const;

  llvm::Error
  foreachSymbolIDInFile(lmdb::Txn &Txn, llvm::StringRef FilePath,
                        llvm::function_ref<void(SymbolID)> Callback) const;

  llvm::Error
  foreachSymbolOccurrence(lmdb::Txn &Txn, SymbolID ID,
                          llvm::function_ref<void(const Ref &)> Callback) const;

  llvm::Error foreachSymbolOccurrenceInFile(
      lmdb::Txn &Txn, HashedID FilePathID,
      llvm::function_ref<void(SymbolID, const Ref &)> Callback) const;

  llvm::Error foreachSymbolOccurrenceInFile(
      lmdb::Txn &Txn, HashedID FilePathID, SymbolID ID,
      llvm::function_ref<void(SymbolID, const Ref &)> Callback) const;

  llvm::Error foreachSymbolOccurrenceInFile(
      lmdb::Txn &Txn, llvm::StringRef FilePath, SymbolID ID,
      llvm::function_ref<void(SymbolID, const Ref &)> Callback) const;

  mutable lmdb::Env DBEnv;
  mutable lmdb::DBI DBIFilePathSymbols;
  mutable lmdb::DBI DBIFilePathRefs;
  mutable lmdb::DBI DBISymbol;
  mutable lmdb::DBI DBISymbolRef;
};
const char *LMDBStorage::FILEURI_SYMBOLS_COLUMN_NAME = "FilePath Symbols";
const char *LMDBStorage::FILEURI_REFS_COLUMN_NAME = "FilePath References";
const char *LMDBStorage::SYMBOLS_COLUMN_NAME = "Symbols";
const char *LMDBStorage::REFS_COLUMN_NAME = "References";

static void reportTime(StringRef Name, llvm::function_ref<void()> F) {
  const auto TimerStart = std::chrono::high_resolution_clock::now();
  F();
  const auto TimerStop = std::chrono::high_resolution_clock::now();
  const auto Duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      TimerStop - TimerStart);
  log("{0} took {1}ms.\n", Name, Duration.count());
}

static HashedID makeHashedID(llvm::StringRef S) {
  HashedID ID;
  auto Hash = llvm::SHA1::hash(llvm::arrayRefFromStringRef(S));
  memcpy(ID.data(), Hash.data(), ID.size());
  return ID;
}

static HashedID readHashedID(llvm::ArrayRef<uint8_t> A) {
  HashedID ID;
  memcpy(ID.data(), A.data(), ID.size());
  return ID;
}

//
// We explicitly disallow failing for BinaryStreamError(),
// as this indicates possible internal corruption of states
// that cannot be recovered, such as tampering of database.
//
// XXX: Should also consider more peaceful failing due to
// schema upgrade.
//
static void handleBinaryStreamErrors(llvm::Error E) {
  llvm::handleAllErrors(
      std::move(E),
      [](std::unique_ptr<llvm::BinaryStreamError> E) -> llvm::Error {
        E->log(llvm::outs());
        return llvm::Error(std::move(E));
      });
}

static void serialize(llvm::BinaryStreamWriter &SW, const SymbolLocation &Loc) {
  handleBinaryStreamErrors(SW.writeCString(Loc.FileURI));
  for (auto &Pos : {Loc.Start, Loc.End}) {
    handleBinaryStreamErrors(SW.writeInteger(Pos.line()));
    handleBinaryStreamErrors(SW.writeInteger(Pos.column()));
  }
}

static void deserialize(llvm::BinaryStreamReader &SR, SymbolLocation &Loc) {
  llvm::StringRef FileURI;
  handleBinaryStreamErrors(SR.readCString(FileURI));
  Loc.FileURI = FileURI.data();
  for (auto Pos : {&Loc.Start, &Loc.End}) {
    uint32_t Line, Column;
    handleBinaryStreamErrors(SR.readInteger(Line));
    handleBinaryStreamErrors(SR.readInteger(Column));
    Pos->setLine(Line);
    Pos->setColumn(Column);
  }
}

static void serialize(llvm::BinaryStreamWriter &SW,
                      const Symbol::IncludeHeaderWithReferences &Include) {
  handleBinaryStreamErrors(SW.writeCString(Include.IncludeHeader));
  handleBinaryStreamErrors(SW.writeInteger(Include.References));
}

static void deserialize(llvm::BinaryStreamReader &SR,
                        Symbol::IncludeHeaderWithReferences &Include) {
  handleBinaryStreamErrors(SR.readCString(Include.IncludeHeader));
  handleBinaryStreamErrors(SR.readInteger(Include.References));
}

static void serialize(llvm::BinaryStreamWriter &SW, const Symbol &Sym) {
  handleBinaryStreamErrors(SW.writeFixedString(Sym.ID.raw()));
  handleBinaryStreamErrors(SW.writeEnum(Sym.SymInfo.Kind));
  handleBinaryStreamErrors(SW.writeEnum(Sym.SymInfo.Lang));
  handleBinaryStreamErrors(SW.writeCString(Sym.Name));
  handleBinaryStreamErrors(SW.writeCString(Sym.Scope));
  serialize(SW, Sym.Definition);
  serialize(SW, Sym.CanonicalDeclaration);
  handleBinaryStreamErrors(SW.writeInteger(Sym.References));
  handleBinaryStreamErrors(SW.writeEnum(Sym.Origin));
  handleBinaryStreamErrors(SW.writeCString(Sym.Signature));
  handleBinaryStreamErrors(SW.writeCString(Sym.CompletionSnippetSuffix));
  handleBinaryStreamErrors(SW.writeCString(Sym.Documentation));
  handleBinaryStreamErrors(SW.writeCString(Sym.ReturnType));
  handleBinaryStreamErrors(SW.writeCString(Sym.Type));
  handleBinaryStreamErrors(SW.writeEnum(Sym.Flags));
  handleBinaryStreamErrors(SW.writeInteger(Sym.IncludeHeaders.size()));
  for (const auto &I : Sym.IncludeHeaders)
    serialize(SW, I);
}

static void deserialize(llvm::BinaryStreamReader &SR, Symbol &Sym) {
  llvm::StringRef Raw;
  handleBinaryStreamErrors(SR.readFixedString(Raw, SymbolID::RawSize));
  Sym.ID = SymbolID::fromRaw(Raw);
  handleBinaryStreamErrors(SR.readEnum(Sym.SymInfo.Kind));
  handleBinaryStreamErrors(SR.readEnum(Sym.SymInfo.Lang));
  handleBinaryStreamErrors(SR.readCString(Sym.Name));
  handleBinaryStreamErrors(SR.readCString(Sym.Scope));
  deserialize(SR, Sym.Definition);
  deserialize(SR, Sym.CanonicalDeclaration);
  handleBinaryStreamErrors(SR.readInteger(Sym.References));
  handleBinaryStreamErrors(SR.readEnum(Sym.Origin));
  handleBinaryStreamErrors(SR.readCString(Sym.Signature));
  handleBinaryStreamErrors(SR.readCString(Sym.CompletionSnippetSuffix));
  handleBinaryStreamErrors(SR.readCString(Sym.Documentation));
  handleBinaryStreamErrors(SR.readCString(Sym.ReturnType));
  handleBinaryStreamErrors(SR.readCString(Sym.Type));
  handleBinaryStreamErrors(SR.readEnum(Sym.Flags));
  size_t Size;
  handleBinaryStreamErrors(SR.readInteger(Size));
  for (size_t i = 0; i < Size; ++i) {
    Symbol::IncludeHeaderWithReferences I;
    deserialize(SR, I);
    Sym.IncludeHeaders.push_back(I);
  }
}

static void serialize(llvm::BinaryStreamWriter &SW, const Ref &Ref) {
  serialize(SW, Ref.Location);
  handleBinaryStreamErrors(SW.writeEnum(Ref.Kind));
}

static void deserialize(llvm::BinaryStreamReader &SR, Ref &Ref) {
  deserialize(SR, Ref.Location);
  handleBinaryStreamErrors(SR.readEnum(Ref.Kind));
}

static void serializeSymbol(std::string &Buf, const Symbol &Sym) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  serialize(SW, Sym);
  handleBinaryStreamErrors(BS.commit());
  Buf = toStringRef(BS.data());
}

static void deserializeSymbol(llvm::StringRef Buf, Symbol &Sym) {
  llvm::BinaryStreamReader SR(Buf, SerializeEndianness);
  deserialize(SR, Sym);
}

static void serializeRef(std::string &Buf, const Ref &R) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  serialize(SW, R);
  handleBinaryStreamErrors(BS.commit());
  Buf = toStringRef(BS.data());
}

static void deserializeRef(llvm::StringRef Buf, Ref &R) {
  llvm::BinaryStreamReader SR(Buf, SerializeEndianness);
  deserialize(SR, R);
}

static void serializeSymbolKey(std::string &Buf, llvm::StringRef FilePath,
                               SymbolID ID) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  handleBinaryStreamErrors(SW.writeCString(FilePath));
  handleBinaryStreamErrors(SW.writeFixedString(ID.raw()));
  handleBinaryStreamErrors(BS.commit());
  Buf = toStringRef(BS.data());
}

static void deserializeSymbolKey(llvm::StringRef Buf, std::string &FilePath,
                                 SymbolID &ID) {
  llvm::BinaryStreamReader SR(Buf, SerializeEndianness);
  llvm::StringRef RawID, StoredFilePath;
  handleBinaryStreamErrors(SR.readCString(StoredFilePath));
  FilePath = StoredFilePath;
  handleBinaryStreamErrors(SR.readFixedString(RawID, SymbolID::RawSize));
  ID = SymbolID::fromRaw(RawID);
}

void OwnSymbol(Symbol &Sym, llvm::StringSaver &StringStore) {
  visitStrings(Sym, [&StringStore](llvm::StringRef &Str) {
    Str = StringStore.save(Str);
  });
}

static void serializeHashedSymbolKey(std::string &Buf, HashedID FilePathID,
                                     SymbolID ID) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  handleBinaryStreamErrors(SW.writeBytes(FilePathID));
  handleBinaryStreamErrors(SW.writeFixedString(ID.raw()));
  handleBinaryStreamErrors(BS.commit());
  Buf = toStringRef(BS.data());
}

static void serializeHashedSymbolKey(std::string &Buf, HashedID FilePathID) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  handleBinaryStreamErrors(SW.writeBytes(FilePathID));
  handleBinaryStreamErrors(BS.commit());
  Buf = toStringRef(BS.data());
}

static void deserializeHashedSymbolKey(llvm::StringRef Buf,
                                       HashedID &FilePathID, SymbolID &ID) {
  llvm::BinaryStreamReader SR(Buf, SerializeEndianness);
  llvm::StringRef RawID;
  llvm::ArrayRef<uint8_t> FPID;
  handleBinaryStreamErrors(SR.readBytes(FPID, FilePathID.size()));
  handleBinaryStreamErrors(SR.readFixedString(RawID, SymbolID::RawSize));
  memcpy(FilePathID.data(), FPID.data(), FilePathID.size());
  ID = SymbolID::fromRaw(RawID);
}

static std::string serializeHashedSymbolKey(HashedID FilePathID, SymbolID ID) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  handleBinaryStreamErrors(SW.writeBytes(FilePathID));
  handleBinaryStreamErrors(SW.writeFixedString(ID.raw()));
  handleBinaryStreamErrors(BS.commit());
  return toStringRef(BS.data());
}

static std::string serializeHashedSymbolKey(HashedID FilePathID) {
  llvm::AppendingBinaryByteStream BS(SerializeEndianness);
  llvm::BinaryStreamWriter SW(BS);
  handleBinaryStreamErrors(SW.writeBytes(FilePathID));
  handleBinaryStreamErrors(BS.commit());
  return toStringRef(BS.data());
}

std::unique_ptr<DatabaseStorage>
DatabaseStorage::open(PathRef DBPath, llvm::StringRef DBEngineName) {
  if (DBEngineName == "LMDB")
    return LMDBStorage::open(DBPath);
  return nullptr;
}

std::unique_ptr<SymbolIndex> DatabaseStorage::getSymbolIndex() const {
  return llvm::make_unique<DatabaseIndex>(this);
}

std::unique_ptr<LMDBStorage> LMDBStorage::open(PathRef DBPath) {
  if (llvm::sys::fs::create_directory(DBPath, true))
    return nullptr;

  llvm::Expected<lmdb::Env> Env = lmdb::Env::create();
  if (llvm::errorToBool(Env.takeError()))
    return nullptr;

  auto Err = Env->setMapsize(1ull << 39);
  if (llvm::errorToBool(std::move(Err)))
    return nullptr;
  Err = Env->setMaxDBs(4);
  if (llvm::errorToBool(std::move(Err)))
    return nullptr;
  Err = Env->open(DBPath.data(), MDB_NOMETASYNC, 0644);
  if (llvm::errorToBool(std::move(Err)))
    return nullptr;

  llvm::Expected<lmdb::Txn> Txn = lmdb::Txn::begin(*Env);
  if (llvm::errorToBool(std::move(Err)))
    return nullptr;

  llvm::Expected<lmdb::DBI> DBIFilePathSymbols = lmdb::DBI::open(
      *Txn, LMDBStorage::FILEURI_SYMBOLS_COLUMN_NAME, MDB_CREATE);
  if (llvm::errorToBool(DBIFilePathSymbols.takeError()))
    return nullptr;
  llvm::Expected<lmdb::DBI> DBIFilePathRefs = lmdb::DBI::open(
      *Txn, LMDBStorage::FILEURI_REFS_COLUMN_NAME, MDB_CREATE | MDB_DUPSORT);
  if (llvm::errorToBool(DBIFilePathRefs.takeError()))
    return nullptr;
  llvm::Expected<lmdb::DBI> DBISymbol = lmdb::DBI::open(
      *Txn, LMDBStorage::SYMBOLS_COLUMN_NAME, MDB_CREATE | MDB_DUPSORT);
  if (llvm::errorToBool(DBIFilePathRefs.takeError()))
    return nullptr;
  llvm::Expected<lmdb::DBI> DBISymbolRef = lmdb::DBI::open(
      *Txn, LMDBStorage::REFS_COLUMN_NAME, MDB_CREATE | MDB_DUPSORT);
  if (llvm::errorToBool(DBIFilePathRefs.takeError()))
    return nullptr;

  Err = Txn->commit();
  if (llvm::errorToBool(std::move(Err)))
    return nullptr;

  auto StorePtr = llvm::make_unique<LMDBStorage>();
  StorePtr->DBEnv = std::move(*Env);
  StorePtr->DBIFilePathSymbols = std::move(*DBIFilePathSymbols);
  StorePtr->DBIFilePathRefs = std::move(*DBIFilePathRefs);
  StorePtr->DBISymbol = std::move(*DBISymbol);
  StorePtr->DBISymbolRef = std::move(*DBISymbolRef);
  return StorePtr;
}

llvm::Error LMDBStorage::removeFile(lmdb::Txn &Txn, llvm::StringRef FilePath) {
  std::vector<SymbolID> SymsToRemove;
  llvm::DenseMap<SymbolID, std::vector<Ref>> RefsToRemove;
  auto Err = foreachSymbolIDInFile(
      Txn, FilePath, [&](SymbolID ID) { SymsToRemove.push_back(ID); });
  if (Err)
    return Err;
  Err = removeSymbol(Txn, FilePath, SymsToRemove);
  if (Err)
    return Err;
  Err = foreachSymbolOccurrenceInFile(
      Txn, FilePath, {},
      [&](SymbolID ID, const Ref &R) { RefsToRemove[ID].push_back(R); });
  if (Err)
    return Err;
  for (auto RP : RefsToRemove) {
    Err = removeRefs(Txn, FilePath, RP.first, RP.second);
    if (Err)
      return Err;
  }
  return llvm::Error::success();
}

llvm::Error LMDBStorage::addFile(lmdb::Txn &Txn, llvm::StringRef FilePath,
                                 const SymbolSlab &Syms, const RefSlab &Refs) {
  if (auto Err = addSymbol(Txn, FilePath, Syms))
    return Err;
  if (auto Err = addRef(Txn, FilePath, Refs))
    return Err;
  return llvm::Error::success();
}

llvm::Error LMDBStorage::addSymbol(lmdb::Txn &Txn, llvm::StringRef FilePath,
                                   const SymbolSlab &Syms) {
  auto AddToDBISymbol = [&Txn, this](llvm::StringRef FilePath,
                                     const Symbol &Sym) -> llvm::Error {
    HashedID FilePathID = makeHashedID(FilePath);
    lmdb::Val Key(Sym.ID.raw()), Data(FilePathID);
    return DBISymbol.put(Txn, Key, Data, 0);
  };
  auto AddToDBIFilePathSymbol = [&Txn, this](llvm::StringRef FilePath,
                                             const Symbol &Sym) -> llvm::Error {
    std::string SerializedSym, SerializedSymKey;
    serializeSymbol(SerializedSym, Sym);
    serializeHashedSymbolKey(SerializedSymKey, makeHashedID(FilePath), Sym.ID);
    lmdb::Val Key(SerializedSymKey), Data(SerializedSym);
    return DBIFilePathSymbols.put(Txn, Key, Data, 0);
  };

  for (auto &Sym : Syms) {
    if (auto Err = AddToDBIFilePathSymbol(FilePath, Sym))
      return Err;
    if (auto Err = AddToDBISymbol(FilePath, Sym))
      return Err;
  }

  return llvm::Error::success();
}

llvm::Error LMDBStorage::removeSymbol(lmdb::Txn &Txn, llvm::StringRef FilePath,
                                      const std::vector<SymbolID> &IDs) {
  HashedID FilePathID = makeHashedID(FilePath);

  for (auto &ID : IDs) {
    std::string SerializedSymKey;
    serializeHashedSymbolKey(SerializedSymKey, makeHashedID(FilePath), ID);
    auto Err = DBIFilePathSymbols.del(Txn, SerializedSymKey, {});
    if (Err) {
      Err = lmdb::filterDBError(std::move(Err), MDB_NOTFOUND);
      if (Err)
        return Err;
      continue;
    }
    Err = DBISymbol.del(Txn, ID.raw(), lmdb::Val(FilePathID));
    if (Err)
      return Err;
  }

  return llvm::Error::success();
}

llvm::Error LMDBStorage::addRef(lmdb::Txn &Txn, llvm::StringRef FilePath,
                                const RefSlab &Refs) {
  HashedID FilePathID = makeHashedID(FilePath);

  for (auto &RefPair : Refs) {
    SymbolID ID = RefPair.first;
    lmdb::Val Key(ID.raw()), Data(FilePathID);
    if (auto Err = DBISymbolRef.put(Txn, Key, Data, 0))
      return Err;

    std::string SerializedKey;
    serializeHashedSymbolKey(SerializedKey, makeHashedID(FilePath), ID);
    for (auto &Ref : RefPair.second) {
      std::string SerializedVal;
      serializeRef(SerializedVal, Ref);
      Key = lmdb::Val(SerializedKey);
      Data = lmdb::Val(SerializedVal);
      if (auto Err = DBIFilePathRefs.put(Txn, Key, Data, 0))
        return Err;
    }
  }

  return llvm::Error::success();
}

llvm::Error LMDBStorage::removeRefs(lmdb::Txn &Txn, llvm::StringRef FilePath,
                                    SymbolID ID, llvm::ArrayRef<Ref> Refs) {
  HashedID FilePathID = makeHashedID(FilePath);

  lmdb::Cursor Cursor;
  {
    auto Expected = lmdb::Cursor::open(Txn, DBIFilePathRefs);
    if (!Expected)
      return Expected.takeError();
    Cursor = std::move(*Expected);
  }

  std::string SerializedKey;
  serializeHashedSymbolKey(SerializedKey, makeHashedID(FilePath), ID);

  for (auto &Ref : Refs) {
    std::string SerializedVal;
    serializeRef(SerializedVal, Ref);
    lmdb::Val Key(SerializedKey), Data(SerializedVal);
    if (auto Err = Cursor.get(Key, Data, MDB_GET_BOTH)) {
      Err = lmdb::filterDBError(std::move(Err), MDB_NOTFOUND);
      if (Err)
        return Err;
      continue;
    }
    if (auto Err = Cursor.del())
      return Err;
  }

  if (auto Err = Cursor.set(SerializedKey)) {
    Err = lmdb::filterDBError(std::move(Err), MDB_NOTFOUND);
    if (Err)
      return Err;
    Err = DBISymbolRef.del(Txn, ID.raw(), lmdb::Val(FilePathID));
    if (Err)
      return Err;
  }
  return llvm::Error::success();
}

llvm::Error LMDBStorage::getSymbol(
    lmdb::Txn &Txn, SymbolID ID,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  lmdb::Cursor Cursor;
  {
    auto Expected = lmdb::Cursor::open(Txn, DBISymbol);
    if (!Expected)
      return Expected.takeError();
    Cursor = std::move(*Expected);
  }

  Symbol Sym;
  Sym.ID = ID;
  llvm::BumpPtrAllocator StringMemPool;
  llvm::StringSaver Strings(StringMemPool);
  llvm::Error E2 = llvm::Error::success();
  llvm::consumeError(std::move(E2));
  auto Err = Cursor.foreachKey(
      ID.raw(),
      [&](const lmdb::Val &Key,
          const lmdb::Val &Data) -> lmdb::IteratorControl {
        lmdb::Val SD;
        E2 = DBIFilePathSymbols.get(
            Txn, serializeHashedSymbolKey(readHashedID(Data), ID), SD);
        if (E2) {
          E2 = lmdb::filterDBError(std::move(E2), MDB_NOTFOUND);
          if (E2)
            return lmdb::IteratorControl::Stop;
          return lmdb::IteratorControl::Continue;
        }

        Symbol S;
        deserializeSymbol(SD, S);
        OwnSymbol(S, Strings);
        Sym = mergeSymbol(Sym, S);
        return lmdb::IteratorControl::Continue;
      });
  if (Err)
    return Err;
  if (!E2)
    Callback(Sym);
  return E2;
}

llvm::Error LMDBStorage::foreachSymbolIDInFile(
    lmdb::Txn &Txn, llvm::StringRef FilePath,
    llvm::function_ref<void(SymbolID)> Callback) const {
  HashedID FilePathID = makeHashedID(FilePath);
  lmdb::Cursor Cursor;
  {
    auto Expected = lmdb::Cursor::open(Txn, DBIFilePathSymbols);
    if (!Expected)
      return Expected.takeError();
    Cursor = std::move(*Expected);
  }

  auto Err = Cursor.foreachRanged(
      serializeHashedSymbolKey(FilePathID),
      [&](const lmdb::Val &Key,
          const lmdb::Val &Data) -> lmdb::IteratorControl {
        SymbolID StoredID;
        HashedID StoredFilePathID;
        deserializeHashedSymbolKey(Key, StoredFilePathID, StoredID);
        if (StoredFilePathID != FilePathID)
          return lmdb::IteratorControl::Stop;
        Callback(StoredID);
        return lmdb::IteratorControl::Continue;
      });
  return Err;
}

llvm::Error LMDBStorage::foreachSymbolOccurrence(
    lmdb::Txn &Txn, SymbolID ID,
    llvm::function_ref<void(const Ref &)> Callback) const {
  lmdb::Cursor Cursor;
  {
    auto Expected = lmdb::Cursor::open(Txn, DBISymbolRef);
    if (!Expected)
      return Expected.takeError();
    Cursor = std::move(*Expected);
  }

  llvm::Error E2 = llvm::Error::success();
  llvm::consumeError(std::move(E2));
  auto Err = Cursor.foreachKey(
      ID.raw(),
      [&](const lmdb::Val &Key,
          const lmdb::Val &Data) -> lmdb::IteratorControl {
        E2 = foreachSymbolOccurrenceInFile(
            Txn, readHashedID(Data), ID,
            [&Callback](SymbolID ID, const Ref &R) { Callback(R); });
        if (E2)
          return lmdb::IteratorControl::Stop;
        return lmdb::IteratorControl::Continue;
      });
  if (Err)
    return Err;
  return E2;
}

llvm::Error LMDBStorage::foreachSymbolOccurrenceInFile(
    lmdb::Txn &Txn, HashedID FilePathID,
    llvm::function_ref<void(SymbolID, const Ref &)> Callback) const {
  lmdb::Cursor Cursor;
  {
    auto Expected = lmdb::Cursor::open(Txn, DBIFilePathRefs);
    if (!Expected)
      return Expected.takeError();
    Cursor = std::move(*Expected);
  }

  auto Err = Cursor.foreachRanged(
      serializeHashedSymbolKey(FilePathID),
      [&](const lmdb::Val &Key,
          const lmdb::Val &Data) -> lmdb::IteratorControl {
        Ref StoredRef;
        SymbolID StoredID;
        HashedID StoredFilePathID;
        deserializeHashedSymbolKey(Key, StoredFilePathID, StoredID);
        deserializeRef(Data, StoredRef);
        if (StoredFilePathID != FilePathID)
          return lmdb::IteratorControl::Stop;
        Callback(StoredID, StoredRef);
        return lmdb::IteratorControl::Continue;
      });
  return Err;
}

llvm::Error LMDBStorage::foreachSymbolOccurrenceInFile(
    lmdb::Txn &Txn, HashedID FilePathID, SymbolID ID,
    llvm::function_ref<void(SymbolID, const Ref &)> Callback) const {
  lmdb::Cursor Cursor;
  {
    auto Expected = lmdb::Cursor::open(Txn, DBIFilePathRefs);
    if (!Expected)
      return Expected.takeError();
    Cursor = std::move(*Expected);
  }

  auto Err = Cursor.foreachKey(
      serializeHashedSymbolKey(FilePathID, ID),
      [&](const lmdb::Val &Key,
          const lmdb::Val &Data) -> lmdb::IteratorControl {
        Ref StoredRef;
        SymbolID StoredID;
        HashedID StoredFilePathID;
        deserializeHashedSymbolKey(Key, StoredFilePathID, StoredID);
        deserializeRef(Data, StoredRef);
        Callback(StoredID, StoredRef);
        return lmdb::IteratorControl::Continue;
      });
  return Err;
}

llvm::Error LMDBStorage::foreachSymbolOccurrenceInFile(
    lmdb::Txn &Txn, llvm::StringRef FilePath, SymbolID ID,
    llvm::function_ref<void(SymbolID, const Ref &)> Callback) const {
  return foreachSymbolOccurrenceInFile(Txn, makeHashedID(FilePath), ID,
                                       Callback);
}

void LMDBStorage::update(PathRef File, std::unique_ptr<SymbolSlab> Syms,
                         std::unique_ptr<RefSlab> Refs) {
  std::string ReportName = "LMDBStorage::update on " + std::string(File);
  reportTime(ReportName, [&]() {
    lmdb::Txn Txn;
    {
      auto Expected = lmdb::Txn::begin(DBEnv);
      if (!Expected) {
        llvm::handleAllErrors(Expected.takeError(),
                              [&](const lmdb::DBError &ErrorInfo) {
                                ErrorInfo.log(llvm::outs());
                              });
        return;
      }
      Txn = std::move(*Expected);
    }

    if (auto Err = removeFile(Txn, File)) {
      llvm::handleAllErrors(
          std::move(Err),
          [&](const lmdb::DBError &ErrorInfo) { ErrorInfo.log(llvm::errs()); });
      return;
    }
    if (auto Err = addFile(Txn, File, *Syms, *Refs)) {
      llvm::handleAllErrors(
          std::move(Err),
          [&](const lmdb::DBError &ErrorInfo) { ErrorInfo.log(llvm::errs()); });
      return;
    }

    if (auto Err = Txn.commit()) {
      llvm::handleAllErrors(
          std::move(Err),
          [&](const lmdb::DBError &ErrorInfo) { ErrorInfo.log(llvm::errs()); });
      return;
    }
  });
}

void LMDBStorage::lookup(
    const LookupRequest &Req,
    llvm::function_ref<void(const Symbol &)> Callback) const {
  lmdb::Txn Txn;
  {
    auto Expected = lmdb::Txn::begin(DBEnv, nullptr, MDB_RDONLY);
    if (!Expected) {
      llvm::handleAllErrors(
          Expected.takeError(),
          [&](const lmdb::DBError &ErrorInfo) { ErrorInfo.log(llvm::outs()); });
      return;
    }
    Txn = std::move(*Expected);
  }

  for (auto ID : Req.IDs)
    consumeError(getSymbol(Txn, ID, Callback));

  if (auto Err = Txn.commit()) {
    llvm::handleAllErrors(std::move(Err), [&](const lmdb::DBError &ErrorInfo) {
      ErrorInfo.log(llvm::errs());
    });
    return;
  }
}

void LMDBStorage::refs(const RefsRequest &Req,
                       llvm::function_ref<void(const Ref &)> Callback) const {
  lmdb::Txn Txn;
  {
    auto Expected = lmdb::Txn::begin(DBEnv, nullptr, MDB_RDONLY);
    if (!Expected) {
      llvm::handleAllErrors(
          Expected.takeError(),
          [&](const lmdb::DBError &ErrorInfo) { ErrorInfo.log(llvm::outs()); });
      return;
    }
    Txn = std::move(*Expected);
  }

  for (auto ID : Req.IDs)
    consumeError(foreachSymbolOccurrence(Txn, ID, Callback));

  if (auto Err = Txn.commit()) {
    llvm::handleAllErrors(std::move(Err), [&](const lmdb::DBError &ErrorInfo) {
      ErrorInfo.log(llvm::errs());
    });
    return;
  }
}

} // namespace clangd
} // namespace clang
