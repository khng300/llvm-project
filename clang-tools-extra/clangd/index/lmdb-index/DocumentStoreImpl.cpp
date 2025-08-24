//===--- DocumentStoreImpl.cpp - Symbol database ----------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "DocumentStoreImpl.h"
#include "Merge.h"
#include "support/Logger.h"
#include "llvm/Support/FileSystem.h"

#include <optional>
#include <system_error>

namespace clang {
namespace clangd {
namespace lmdb_index {

llvm::ErrorOr<std::unique_ptr<DocumentStoreImpl::TransactionImpl>>
DocumentStoreImpl::TransactionImpl::begin(DocumentStoreImpl &DB, bool RO,
                                          llvm::StringRef Tag) {
  TxnLocker TxnLk = [&]() -> TxnLocker {
    if (RO)
      return std::shared_lock<std::shared_mutex>(DB.TransactionMu);
    return std::unique_lock<std::shared_mutex>(DB.TransactionMu);
  }();
  llvm::ErrorOr<lmdb::Txn> TxnOrErr{std::error_code()};
  do {
    TxnOrErr = lmdb::Txn::begin(DB.Env, RO ? MDB_RDONLY : 0);
    if (!TxnOrErr) {
      auto EC = TxnOrErr.getError();
      if (EC != lmdb::DBError::MapResized)
        return EC;
      // Refresh the map size as it was changed outside our process.
      DB.Env.setMapSize(0);
      DB.refreshEnvMapSize();
    }
  } while (!TxnOrErr);
  return std::unique_ptr<TransactionImpl>(
      new TransactionImpl(DB, std::move(TxnLk), std::move(*TxnOrErr), Tag));
}

std::error_code DocumentStoreImpl::TransactionImpl::commit() && {
  auto StartCommitTime = std::chrono::system_clock::now();
  auto EC = mapDBErrorToIndexDBError(Txn.commit());
  if (EC)
    return EC;
  auto EndCommitTime = std::chrono::system_clock::now();

  log("commit(): Tag = {0}:\n"
      "\tIndexDuration = {1}\n"
      "\tDatafileDuration = {2}\n"
      "\tUpdatedSymbolsDuration = {3}\n"
      "\tCommitDuration = {4}",
      Tag, IndexDuration, DatafileDuration, UpdatedSymbolsDuration,
      EndCommitTime - StartCommitTime);
  return {};
}

void DocumentStoreImpl::TransactionImpl::abort() && { Txn.abort(); }

void DocumentStoreImpl::TransactionImpl::raiseFatalError(std::error_code EC) {
  DB->raiseFatalError(EC);
}

llvm::ErrorOr<OID> DocumentStoreImpl::TransactionImpl::createEmptyIndexDataFile(
    llvm::StringRef ShardPath, llvm::StringRef FileURI) {
  cl::IndexDataFile IDFile = [&]() {
    cl::IndexDataFile Ret;
    Ret.ShardPath = ShardPath;
    Ret.FileURI = FileURI;
    Ret.ShardPathHash = generateObjectHash(ShardPath);
    Ret.FileURIHash = generateObjectHash(FileURI);
    return Ret;
  }();
  return createIndexDataFile(IDFile);
}

llvm::ErrorOr<cl::IndexDataFile>
DocumentStoreImpl::TransactionImpl::toIndexDataFile(llvm::StringRef ShardPath,
                                                    const IndexFileOut &Shard,
                                                    int64_t ModifiedTime) {
  cl::IndexDataFile Ret;
  URI Uri = URI::createFile(ShardPath);
  Ret.ShardPath = ShardPath;
  Ret.FileURI = Uri.toString();
  Ret.ShardPathHash = generateObjectHash(ShardPath);
  Ret.FileURIHash = generateObjectHash(Ret.FileURI.view());
  Ret.ModifiedTime = ModifiedTime;
  Ret.Valid = true;
  if (Shard.Cmd)
    Ret.Cmd = Marshaller.fromNative(*Shard.Cmd);
  if (Shard.Sources)
    Ret.Sources = Marshaller.fromNative(*Shard.Sources);
  if (Shard.Symbols) {
    for (const auto &Sym : *Shard.Symbols) {
      auto Res = Marshaller.fromNative(Sym);
      if (!Res)
        return Res.getError();
      Ret.Symbols[Res->ID] = *Res;
    }
  }
  if (Shard.Refs) {
    for (const auto &RefList : *Shard.Refs) {
      for (const auto &Ref : RefList.second) {
        auto Res = Marshaller.fromNative(Ref);
        if (!Res)
          return Res.getError();
        Ret.Refs[Marshaller.fromNative(RefList.first)].emplace_back(*Res);
        if (Ref.Kind == RefKind::Unknown)
          continue;
        auto Res2 = Marshaller.fromNative(
            ContainedRefsResult{Ref.Location, Ref.Kind, RefList.first});
        if (!Res2)
          return Res2.getError();
        Ret.RevRefs[Res->Container].emplace_back(*Res2);
      }
    }
  }
  if (Shard.Relations) {
    for (const auto &Relation : *Shard.Relations) {
      auto Res = Marshaller.fromNative(Relation);
      Ret.Relations[{Res.Subject, Res.Predicate}].emplace_back(Res);
    }
  }
  return Ret;
}

llvm::ErrorOr<OID> DocumentStoreImpl::TransactionImpl::createIndexDataFile(
    const cl::IndexDataFile &IDFile) {
  auto OidOrErr = DB->IndexDataFilesDB.allocOid(Txn, IDFile);
  if (!OidOrErr)
    return OidOrErr.getError();
  return OidOrErr;
}

llvm::ErrorOr<const cl::IndexDataFile *>
DocumentStoreImpl::TransactionImpl::getIndexDataFile(OID Oid) {
  return DB->IndexDataFilesDB.getOidPointer(Txn, Oid);
}

llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
DocumentStoreImpl::TransactionImpl::getIndexDataFileByShardPath(
    llvm::StringRef ShardPath) {
  auto ItOrErr = DB->IndexDataFilesDB.findOid(
      Txn, key_id::IDXDATAFILE_SHARDPATH,
      lmdb::makeSlice(generateObjectHash(ShardPath)));
  if (!ItOrErr)
    return ItOrErr.getError();
  for (auto Res = ItOrErr->peek(); Res; Res = ItOrErr->next()) {
    auto &Oid = *Res;
    auto IDFile = DB->IndexDataFilesDB.getOidPointer(Txn, Oid);
    if (!IDFile)
      return IDFile.getError();
    if ((*IDFile)->ShardPath == ShardPath)
      return std::pair{Oid, *IDFile};
  }
  if (auto EC = ItOrErr->getLastError())
    return mapDBErrorToIndexDBError(EC);
  return IndexDBError::Notfound;
}

llvm::ErrorOr<std::pair<OID, const cl::IndexDataFile *>>
DocumentStoreImpl::TransactionImpl::getIndexDataFileByURI(
    llvm::StringRef FileURI) {
  auto ItOrErr = DB->IndexDataFilesDB.findOid(
      Txn, key_id::IDXDATAFILE_URI,
      lmdb::makeSlice(generateObjectHash(FileURI)));
  if (!ItOrErr)
    return ItOrErr.getError();
  for (auto Res = ItOrErr->peek(); Res; Res = ItOrErr->next()) {
    auto &Oid = *Res;
    auto IDFile = DB->IndexDataFilesDB.getOidPointer(Txn, Oid);
    if (!IDFile)
      return IDFile.getError();
    if ((*IDFile)->FileURI == FileURI)
      return std::pair{Oid, *IDFile};
  }
  if (auto EC = ItOrErr->getLastError())
    return mapDBErrorToIndexDBError(EC);
  return IndexDBError::Notfound;
}

std::error_code
DocumentStoreImpl::TransactionImpl::removeIndexDataFile(OID Oid) {
  auto EC = DB->IndexDataFilesDB.removeOid(Txn, Oid);
  if (EC)
    return EC;
  return {};
}

std::error_code DocumentStoreImpl::TransactionImpl::writeIndexDataFile(
    OID Oid, const cl::IndexDataFile &IDFile) {
  if (auto EC = DB->IndexDataFilesDB.writeOid(Txn, Oid, IDFile))
    return EC;
  return {};
}

std::unique_ptr<IndexFileInfo>
DocumentStoreImpl::TransactionImpl::loadShardInfo(llvm::StringRef ShardPath) {
  auto Res = getIndexDataFileByShardPath(ShardPath);
  if (!Res || !Res->second->Valid)
    return nullptr;
  const cl::IndexDataFile &IDFile = *Res->second;
  IndexFileInfo IFileInfo;
  IFileInfo.FileURI = IDFile.FileURI;
  IFileInfo.ShardPath = IDFile.ShardPath;
  IFileInfo.ModifiedTime = IDFile.ModifiedTime;
  if (IDFile.Sources)
    IFileInfo.Sources = Marshaller.toNative(*IDFile.Sources);
  if (IDFile.Cmd)
    IFileInfo.Cmd = Marshaller.toNative(*IDFile.Cmd);
  return std::make_unique<IndexFileInfo>(std::move(IFileInfo));
}

llvm::ErrorOr<OID> DocumentStoreImpl::TransactionImpl::ensureShardExistence(
    llvm::StringRef FileURI) {
  auto Res = getIndexDataFileByURI(FileURI);
  if (!Res) {
    auto PathOrErr = URI::resolve(FileURI);
    if (!PathOrErr) {
      elog("ensureShardExistence: Invalid FileURI encountered: {0}", FileURI);
      return IndexDBError::InternalError;
    }
    return createEmptyIndexDataFile(*PathOrErr, FileURI);
  }
  return Res->first;
}

llvm::ErrorOr<cl::Symbol>
DocumentStoreImpl::TransactionImpl::lookupShardsSymbol(SymbolID ID) {
  auto CurOrErr = lmdb::Cursor::open(Txn, DB->SymbolIDToIDFileDbi);
  if (!CurOrErr)
    return mapDBErrorToIndexDBError(CurOrErr.getError());
  auto &Cursor = *CurOrErr;
  cl::SymbolID SymId = Marshaller.fromNative(ID);
  Slice Key = lmdb::makeSlice(SymId);
  auto EC = Cursor.get(Key, MDB_SET);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  llvm::SmallVector<const cl::Symbol *> Syms;
  auto It = lmdb::DupIterator(std::move(*CurOrErr));
  std::optional<Slice> Data;
  while ((Data = It.next())) {
    auto Oid = Data->toTypeRef<OID>();
    auto IDFileOrErr = getIndexDataFile(Oid);
    if (!IDFileOrErr)
      return IndexDBError::DBInconsistent;
    const cl::IndexDataFile &IDFile = **IDFileOrErr;
    auto SymIt = IDFile.Symbols.find(SymId);
    if (SymIt == IDFile.Symbols.end())
      return IndexDBError::DBInconsistent;
    Syms.emplace_back(&SymIt->second);
  }
  if ((EC = It.errorCode()))
    DB->raiseFatalError(mapDBErrorToIndexDBError(EC));
  assert(!SymStorage.empty());
  cl::Symbol Sym;
  for (const auto &V : Syms)
    Sym = Sym.ID ? mergeSymbol(Sym, *V) : *V;
  return Sym;
}

llvm::ErrorOr<cl::IndexedSymbol>
DocumentStoreImpl::TransactionImpl::findSymbol(SymbolID ID) {
  auto ItOrErr =
      DB->IndexedSymbolsDB.findOid(Txn, key_id::INDEXEDSYMBOL_ID, ID.raw());
  if (!ItOrErr)
    return ItOrErr.getError();
  auto Oid = ItOrErr->peek();
  if (!Oid)
    return IndexDBError::Corrupted;
  auto IndexedSym = DB->IndexedSymbolsDB.readOid(Txn, *Oid);
  if (!IndexedSym)
    return IndexDBError::Corrupted;
  return *IndexedSym;
}

llvm::ErrorOr<lmdb::Cursor>
DocumentStoreImpl::TransactionImpl::getKeyToFileDbiCursor(lmdb::DBI &Dbi,
                                                          Slice Key) {
  auto CurOrErr = lmdb::Cursor::open(Txn, Dbi);
  if (!CurOrErr)
    DB->raiseFatalError(mapDBErrorToIndexDBError(CurOrErr.getError()));
  auto &Cursor = *CurOrErr;
  auto EC = Cursor.get(Key, MDB_SET);
  if (EC)
    return mapDBErrorToIndexDBError(EC);
  return std::move(Cursor);
}

std::error_code DocumentStoreImpl::TransactionImpl::insertShardIndexes(
    OID ShardOid, const cl::IndexDataFile &IDFile) {
  Slice OidSlice = makeSlice(ShardOid);
  auto Inserter = [&](lmdb::DBI &IdxDbi, const auto &Table) -> std::error_code {
    for (const auto &[Key, _] : Table) {
      Slice KeySlice = makeSlice(Key);
      if (auto EC = IdxDbi.put(Txn, KeySlice, OidSlice))
        return mapDBErrorToIndexDBError(EC);
    }
    return {};
  };

  auto EC = Inserter(DB->SymbolIDToIDFileDbi, IDFile.Symbols);
  if (EC)
    return EC;
  EC = Inserter(DB->RefSymbolIDToIDFileDbi, IDFile.Refs);
  if (EC)
    return EC;
  EC = Inserter(DB->RelationKeyToIDFileDbi, IDFile.Relations);
  if (EC)
    return EC;
  EC = Inserter(DB->RevRefSymbolIDToIDFileDbi, IDFile.RevRefs);
  if (EC)
    return EC;
  return {};
}

std::error_code DocumentStoreImpl::TransactionImpl::removeShardIndexes(
    OID ShardOid, const cl::IndexDataFile &IDFile) {
  Slice OidSlice = makeSlice(ShardOid);
  auto Remover = [&](lmdb::DBI &IdxDbi, const auto &Table) -> std::error_code {
    for (const auto &[Key, _] : Table) {
      Slice KeySlice = makeSlice(Key);
      if (auto EC = IdxDbi.del(Txn, KeySlice, OidSlice))
        return mapDBErrorToIndexDBError(EC);
    }
    return {};
  };

  auto EC = Remover(DB->SymbolIDToIDFileDbi, IDFile.Symbols);
  if (EC)
    return EC;
  EC = Remover(DB->RefSymbolIDToIDFileDbi, IDFile.Refs);
  if (EC)
    return EC;
  EC = Remover(DB->RelationKeyToIDFileDbi, IDFile.Relations);
  if (EC)
    return EC;
  EC = Remover(DB->RevRefSymbolIDToIDFileDbi, IDFile.RevRefs);
  if (EC)
    return EC;
  return {};
}

std::error_code
DocumentStoreImpl::TransactionImpl::updateShard(llvm::StringRef ShardPath,
                                                const IndexFileOut *Shard,
                                                int64_t ModifiedTime) {
  trace::Span("LMDB::Txn::updateShard");
  bool RemoveShard = Shard == nullptr;

  auto PrevShard = getIndexDataFileByShardPath(ShardPath);
  if (!PrevShard) {
    // No corresponding old shard, thus the error must be IndexDBError::Notfound
    // or it indicates error.
    if (auto EC = PrevShard.getError(); EC != IndexDBError::Notfound)
      return EC;
  } else {
    // Old shard's data needs to be removed first.
    OID Oid = PrevShard->first;
    auto StartTime = std::chrono::system_clock::now();
    std::error_code EC = removeShardIndexes(Oid, *PrevShard->second);
    if (EC)
      return EC;
    IndexDuration += std::chrono::system_clock::now() - StartTime;

    StartTime = std::chrono::system_clock::now();
    for (const auto &[SymId, _] : PrevShard->second->Symbols) {
      Slice EmptySlice;
      EC = DB->UpdatedSymbolsDbi.put(Txn, makeSlice(SymId), EmptySlice,
                                     MDB_NOOVERWRITE);
      if (EC && EC != lmdb::DBError::Keyexist)
        return mapDBErrorToIndexDBError(EC);
    }
    UpdatedSymbolsDuration += std::chrono::system_clock::now() - StartTime;

    if (RemoveShard) {
      StartTime = std::chrono::system_clock::now();
      EC = removeIndexDataFile(Oid);
      if (EC)
        return EC;
      DatafileDuration += std::chrono::system_clock::now() - StartTime;
    }
  }

  if (!RemoveShard) {
    auto StartTime = std::chrono::system_clock::now();
    llvm::ErrorOr<OID> Oid =
        ensureShardExistence(URI::createFile(ShardPath).toString());
    if (!Oid)
      return Oid.getError();
    auto IDFileOrErr = toIndexDataFile(ShardPath, *Shard, ModifiedTime);
    if (!IDFileOrErr)
      return IDFileOrErr.getError();
    const cl::IndexDataFile &IDFile = *IDFileOrErr;
    auto EC = writeIndexDataFile(*Oid, IDFile);
    if (EC)
      return EC;
    DatafileDuration += std::chrono::system_clock::now() - StartTime;

    StartTime = std::chrono::system_clock::now();
    EC = insertShardIndexes(*Oid, IDFile);
    if (EC)
      return EC;
    IndexDuration += std::chrono::system_clock::now() - StartTime;

    StartTime = std::chrono::system_clock::now();
    for (const auto &[SymId, _] : IDFile.Symbols) {
      Slice EmptySlice;
      EC = DB->UpdatedSymbolsDbi.put(Txn, makeSlice(SymId), EmptySlice,
                                     MDB_NOOVERWRITE);
      if (EC && EC != lmdb::DBError::Keyexist)
        return mapDBErrorToIndexDBError(EC);
    }
    UpdatedSymbolsDuration += std::chrono::system_clock::now() - StartTime;
  }

  return {};
}

std::error_code DocumentStoreImpl::TransactionImpl::updateSymbol(SymbolID ID) {
  std::optional<OID> ExistingOid;
  if (auto ItOrErr = DB->IndexedSymbolsDB.findOid(Txn, key_id::INDEXEDSYMBOL_ID,
                                                  ID.raw())) {
    auto Res = ItOrErr->peek();
    if (!Res)
      return IndexDBError::DBInconsistent;
    ExistingOid = *Res;
  } else {
    // No corresponding existing IndexedSymbol, thus the error must be
    // IndexDBError::Notfound or it indicates error.
    if (auto EC = ItOrErr.getError(); EC != IndexDBError::Notfound)
      return EC;
  }

  auto SerializedSym = lookupShardsSymbol(ID);
  if (!SerializedSym) {
    auto EC = SerializedSym.getError();
    if (EC != IndexDBError::Notfound)
      return EC;
    if (ExistingOid) {
      EC = DB->IndexedSymbolsDB.removeOid(Txn, *ExistingOid);
      if (EC)
        return EC;
    }
    return {};
  }
  auto SymOrErr = Marshaller.toNative(*SerializedSym);
  if (!SymOrErr)
    return SymOrErr.getError();
  cl::IndexedSymbol IndexedSym = [&]() {
    std::vector<Token> Tokens = buildTokens(*SymOrErr);
    cl::data::vector<uint64_t> HashedTokens;
    HashedTokens.reserve(Tokens.size());
    for (const auto &Tok : Tokens)
      HashedTokens.emplace_back(Tok.getXxHash());
    return cl::IndexedSymbol{*SerializedSym, std::move(HashedTokens)};
  }();
  if (!ExistingOid)
    return DB->IndexedSymbolsDB.allocOid(Txn, IndexedSym).getError();
  return DB->IndexedSymbolsDB.writeOid(Txn, *ExistingOid, IndexedSym);
}

std::error_code DocumentStoreImpl::TransactionImpl::updateIndexedSymbols() {
  auto Stat = DB->UpdatedSymbolsDbi.stat(Txn);
  if (!Stat)
    return mapDBErrorToIndexDBError(Stat.getError());
  if (Stat->ms_entries != 0) {
    auto CurOrErr = lmdb::Cursor::open(Txn, DB->UpdatedSymbolsDbi);
    if (!CurOrErr)
      return mapDBErrorToIndexDBError(CurOrErr.getError());
    auto &Cursor = *CurOrErr;
    Slice Key;
    std::error_code EC;
    while (!(EC = Cursor.get(Key, MDB_NEXT))) {
      SymbolID SymID = Marshaller.toNative(Key.toTypeRef<cl::SymbolID>());
      EC = updateSymbol(SymID);
      if (EC)
        return EC;
    }
    assert(EC);
    if (EC != lmdb::DBError::Notfound)
      return mapDBErrorToIndexDBError(EC);
    EC = DB->UpdatedSymbolsDbi.drop(Txn);
    if (EC)
      return mapDBErrorToIndexDBError(EC);
  }
  return {};
}

DocumentStoreImpl::DocumentStoreImpl(llvm::StringRef Path) {
  auto EnvOrErr = lmdb::Environment::create();
  if (!EnvOrErr)
    return;
  Env = std::move(*EnvOrErr);

  // Maximum 32 dbs for now.
  Env.setMaxDBs(32);

  unsigned int OpenFlags = MDB_NOTLS | MDB_NOMETASYNC | MDB_WRITEMAP;
#ifdef MDB_WRITEMAP_FSYNC
  OpenFlags |= MDB_WRITEMAP_FSYNC;
#endif
  Env.open(Path.data(), OpenFlags);

  auto TxnOrErr = lmdb::Txn::begin(Env);
  if (!TxnOrErr)
    return;
  refreshEnvMapSize();
  auto Txn = std::move(*TxnOrErr);

  auto EC = IndexDataFilesDB.open(Txn);
  if (EC)
    return;
  EC = IndexedSymbolsDB.open(Txn);
  if (EC)
    return;

  auto DbiFlags = MDB_DUPSORT | MDB_DUPFIXED;
  auto DbiOrErr =
      lmdb::DBI::open(Txn, "SymbolIDToIDFileDbi",
                      DbiFlags | MDB_INTEGERKEY | MDB_INTEGERDUP | MDB_CREATE);
  if (!DbiOrErr)
    return;
  SymbolIDToIDFileDbi = *DbiOrErr;

  DbiOrErr =
      lmdb::DBI::open(Txn, "RefSymbolIDToIDFileDbi",
                      DbiFlags | MDB_INTEGERKEY | MDB_INTEGERDUP | MDB_CREATE);
  if (!DbiOrErr)
    return;
  RefSymbolIDToIDFileDbi = *DbiOrErr;

  DbiOrErr = lmdb::DBI::open(Txn, "RelationKeyToIDFileDbi",
                             DbiFlags | MDB_INTEGERDUP | MDB_CREATE);
  if (!DbiOrErr)
    return;
  RelationKeyToIDFileDbi = *DbiOrErr;

  DbiOrErr =
      lmdb::DBI::open(Txn, "RevRefSymbolIDToIDFileDbi",
                      DbiFlags | MDB_INTEGERKEY | MDB_INTEGERDUP | MDB_CREATE);
  if (!DbiOrErr)
    return;
  RevRefSymbolIDToIDFileDbi = *DbiOrErr;

  DbiOrErr =
      lmdb::DBI::open(Txn, "UpdatedSymbolsDbi", MDB_INTEGERKEY | MDB_CREATE);
  if (!DbiOrErr)
    return;
  UpdatedSymbolsDbi = *DbiOrErr;

  Txn.commit();
}

DocumentStoreImpl::~DocumentStoreImpl() {
  ::MDB_envinfo EnvInfo = Env.getEnvInfo();
  log("~LMDBInstance(): mapsize = {0}, last_pgno = {1}, last_txnid = {2}",
      EnvInfo.me_mapsize, EnvInfo.me_last_pgno, EnvInfo.me_last_txnid);
}

std::shared_ptr<DocumentStoreImpl>
DocumentStoreImpl::createInstance(llvm::StringRef Path) {
  return std::shared_ptr<DocumentStoreImpl>(new DocumentStoreImpl(Path));
}

void DocumentStoreImpl::raiseFatalError(std::error_code EC) {
  llvm::report_fatal_error(
      llvm::Twine("Database fatal error! Error message: " + EC.message()));
}

llvm::ErrorOr<std::unique_ptr<Transaction>>
DocumentStoreImpl::beginTransaction(bool RO, llvm::StringRef Tag) {
  auto Txn = TransactionImpl::begin(*this, RO, Tag);
  if (!Txn)
    return Txn.getError();
  return std::move(*Txn);
}

size_t DocumentStoreImpl::databaseMapSize() { return EnvMapSize; }

std::error_code DocumentStoreImpl::growDatabaseMapSize() {
  constexpr mdb_size_t SizeCap =
      mdb_size_t(2) * 1024 * 1024 * 1024 * 1024; // 2TB for now.
  size_t MapSize = EnvMapSize;
  std::unique_lock<std::shared_mutex> Lk(TransactionMu);
  if (MapSize < EnvMapSize) {
    // Other threads already updated the database size, so avoid repeating it.
    return {};
  }
  if (auto EC = Env.setMapSize(std::min(EnvMapSize << 1, SizeCap)))
    return EC;
  refreshEnvMapSize();
  return {};
}

void DocumentStoreImpl::refreshEnvMapSize() {
  EnvMapSize = Env.getEnvInfo().me_mapsize;
}

std::unique_ptr<IndexFileInfo>
DocumentStoreImpl::loadShardInfo(Transaction &TransIn,
                                 llvm::StringRef ShardPath) {
  TransactionImpl &Txn = static_cast<TransactionImpl &>(TransIn);
  return Txn.loadShardInfo(ShardPath);
}

std::error_code DocumentStoreImpl::updateShard(Transaction &TransIn,
                                               llvm::StringRef ShardPath,
                                               const IndexFileOut *Shard,
                                               int64_t ModifiedTime) {
  TransactionImpl &Txn = static_cast<TransactionImpl &>(TransIn);
  return Txn.updateShard(ShardPath, Shard, ModifiedTime);
}

std::shared_ptr<DocumentStore>
createSharedDatabase(llvm::StringRef ProjectPath) {
  llvm::SmallString<16> DatabasePath = ProjectPath;
  if (DatabasePath.empty()) {
    if (!llvm::sys::path::cache_directory(DatabasePath))
      return nullptr;
  } else {
    llvm::sys::path::append(DatabasePath, ".cache");
  }
  llvm::sys::path::append(DatabasePath, "clangd", "db-index");
  if (llvm::sys::fs::create_directories(DatabasePath))
    return nullptr;
  return DocumentStoreImpl::createInstance(DatabasePath);
}

} // namespace lmdb_index
} // namespace clangd
} // namespace clang
