//== BackgroundIndexStorage.cpp - Provide caching support to BackgroundIndex ==/
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "GlobalCompilationDatabase.h"
#include "Logger.h"
#include "Path.h"
#include "index/Background.h"
#include "index/dbindex/DbIndex.h"
#include "llvm/ADT/Optional.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/ScopeExit.h"
#include "llvm/ADT/SmallString.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/FileUtilities.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/Path.h"
#include "llvm/Support/Process.h"
#include <functional>

namespace clang {
namespace clangd {
namespace {

std::string getShardPathFromFilePath(llvm::StringRef ShardRoot,
                                     llvm::StringRef FilePath) {
  llvm::SmallString<128> ShardRootSS(ShardRoot);
  llvm::sys::path::append(ShardRootSS, llvm::sys::path::filename(FilePath) +
                                           "." + llvm::toHex(digest(FilePath)) +
                                           ".idx");
  return ShardRootSS.str();
}

// Uses disk as a storage for index shards. Creates a directory called
// ".clangd/index/" under the path provided during construction.
class DiskBackedIndexStorage : public BackgroundIndexStorage {
  std::string DiskShardRoot;

public:
  // Sets DiskShardRoot to (Directory + ".clangd/index/") which is the base
  // directory for all shard files.
  DiskBackedIndexStorage(llvm::StringRef Directory) {
    llvm::SmallString<128> CDBDirectory(Directory);
    llvm::sys::path::append(CDBDirectory, ".clangd", "index");
    DiskShardRoot = CDBDirectory.str();
    std::error_code OK;
    std::error_code EC = llvm::sys::fs::create_directories(DiskShardRoot);
    if (EC != OK) {
      elog("Failed to create directory {0} for index storage: {1}",
           DiskShardRoot, EC.message());
    }
  }

  std::unique_ptr<IndexFileIn>
  loadShard(llvm::StringRef ShardIdentifier) const override {
    const std::string ShardPath =
        getShardPathFromFilePath(DiskShardRoot, ShardIdentifier);
    auto Buffer = llvm::MemoryBuffer::getFile(ShardPath);
    if (!Buffer)
      return nullptr;
    if (auto I = readIndexFile(Buffer->get()->getBuffer()))
      return std::make_unique<IndexFileIn>(std::move(*I));
    else
      elog("Error while reading shard {0}: {1}", ShardIdentifier,
           I.takeError());
    return nullptr;
  }

  llvm::Error storeShard(llvm::StringRef ShardIdentifier,
                         IndexFileOut Shard) const override {
    auto ShardPath = getShardPathFromFilePath(DiskShardRoot, ShardIdentifier);
    return llvm::writeFileAtomically(ShardPath + ".tmp.%%%%%%%%", ShardPath,
                                     [&Shard](llvm::raw_ostream &OS) {
                                       OS << Shard;
                                       return llvm::Error::success();
                                     });
  }
};

// Doesn't persist index shards anywhere (used when the CDB dir is unknown).
// We could consider indexing into ~/.clangd/ or so instead.
class NullStorage : public BackgroundIndexStorage {
public:
  std::unique_ptr<IndexFileIn>
  loadShard(llvm::StringRef ShardIdentifier) const override {
    return nullptr;
  }

  llvm::Error storeShard(llvm::StringRef ShardIdentifier,
                         IndexFileOut Shard) const override {
    vlog("Couldn't find project for {0}, indexing in-memory only",
         ShardIdentifier);
    return llvm::Error::success();
  }
};

// Persist shards in DbIndex
class DbIndexBackedIndexStorage : public BackgroundIndexStorage {
public:
  std::shared_ptr<dbindex::LMDBIndex> DB;

public:
  // Sets DiskShardRoot to (Directory + ".clangd/index/") which is the base
  // directory for all shard files.
  DbIndexBackedIndexStorage(std::shared_ptr<dbindex::LMDBIndex> DB) : DB(DB) {}

  std::unique_ptr<IndexFileIn>
  loadShard(llvm::StringRef ShardIdentifier) const override {
    if (!DB)
      return nullptr;
    auto I = DB->get(ShardIdentifier);
    if (!I) {
      elog("Error while reading shard {0}: {1}", ShardIdentifier,
           I.takeError());
      return nullptr;
    }
    return std::make_unique<IndexFileIn>(std::move(*I));
  }

  llvm::Error storeShard(llvm::StringRef ShardIdentifier,
                         IndexFileOut Shard) const override {
    if (!DB)
      return llvm::Error::success();
    return DB->update(ShardIdentifier, Shard);
  }
};

// Creates and owns IndexStorages for multiple CDBs.
class DiskBackedIndexStorageManager {
public:
  DiskBackedIndexStorageManager(
      std::function<llvm::Optional<ProjectInfo>(PathRef)> GetProjectInfo)
      : IndexStorageMapMu(std::make_unique<std::mutex>()),
        GetProjectInfo(std::move(GetProjectInfo)) {
    llvm::SmallString<128> HomeDir;
    llvm::sys::path::home_directory(HomeDir);
    this->HomeDir = HomeDir.str().str();
  }

  // Creates or fetches to storage from cache for the specified project.
  BackgroundIndexStorage *operator()(PathRef File) {
    std::lock_guard<std::mutex> Lock(*IndexStorageMapMu);
    Path CDBDirectory = HomeDir;
    if (auto PI = GetProjectInfo(File))
      CDBDirectory = PI->SourceRoot;
    auto &IndexStorage = IndexStorageMap[CDBDirectory];
    if (!IndexStorage)
      IndexStorage = create(CDBDirectory);
    return IndexStorage.get();
  }

private:
  std::unique_ptr<BackgroundIndexStorage> create(PathRef CDBDirectory) {
    if (CDBDirectory.empty()) {
      elog("Tried to create storage for empty directory!");
      return std::make_unique<NullStorage>();
    }
    return std::make_unique<DiskBackedIndexStorage>(CDBDirectory);
  }

  Path HomeDir;

  llvm::StringMap<std::unique_ptr<BackgroundIndexStorage>> IndexStorageMap;
  std::unique_ptr<std::mutex> IndexStorageMapMu;

  std::function<llvm::Optional<ProjectInfo>(PathRef)> GetProjectInfo;
};

// Creates and owns IndexStorages for multiple CDBs.
class DbIndexBackedIndexStorageManager {
public:
  DbIndexBackedIndexStorageManager(
      std::function<llvm::Optional<ProjectInfo>(PathRef)> GetProjectInfo,
      llvm::StringRef WorkspaceRoot, std::shared_ptr<dbindex::LMDBIndex> &Index)
      : IndexStorage(std::make_unique<NullStorage>()),
        GetProjectInfo(std::move(GetProjectInfo)) {
    llvm::SmallString<128> Dir, WorkDir;
    if (WorkspaceRoot.size())
      WorkDir = WorkspaceRoot;
    else
      llvm::sys::fs::current_path(WorkDir);

    if (auto EnvPrefix = llvm::sys::Process::GetEnv("CLANGD_INDEXDB_PREFIX")) {
      Dir = *EnvPrefix;
    } else if (llvm::sys::Process::GetEnv("CLANGD_INDEXDB_PREFIX_HOME")) {
      if (llvm::sys::path::home_directory(Dir))
        llvm::sys::path::append(Dir, ".clangd");
    }

    if (Dir.size()) {
      llvm::sys::path::append(Dir, WorkDir);
    } else {
      Dir = WorkDir;
      llvm::sys::path::append(Dir, ".clangd", "IndexDB");
    }

    DbIndexRoot = Dir.str();

    std::error_code EC = llvm::sys::fs::create_directories(DbIndexRoot, true);
    if (EC) {
      elog("Failed to create directory {0} for IndexDB storage: {1}",
           DbIndexRoot, EC.message());
      return;
    }
    auto E = dbindex::LMDBIndex::open(DbIndexRoot);
    if (!E) {
      elog("Error while opening database {0}: {1}", DbIndexRoot, E.takeError());
      return;
    }
    DB = std::move(*E);
    IndexStorage = std::make_unique<DbIndexBackedIndexStorage>(DB);
    Index = DB;
  }

  // Creates or fetches to storage from cache for the specified project.
  BackgroundIndexStorage *operator()(PathRef File) {
    return IndexStorage.get();
  }

private:
  std::shared_ptr<dbindex::LMDBIndex> DB;
  std::string DbIndexRoot;
  std::unique_ptr<BackgroundIndexStorage> IndexStorage;
  std::function<llvm::Optional<ProjectInfo>(PathRef)> GetProjectInfo;
};

} // namespace

BackgroundIndexStorage::Factory
BackgroundIndexStorage::createDiskBackedStorageFactory(
    std::function<llvm::Optional<ProjectInfo>(PathRef)> GetProjectInfo) {
  return DiskBackedIndexStorageManager(std::move(GetProjectInfo));
}

BackgroundIndexStorage::Factory
BackgroundIndexStorage::createDbIndexBackedStorageFactory(
    std::function<llvm::Optional<ProjectInfo>(PathRef)> GetProjectInfo,
    llvm::StringRef WorkspaceRoot, std::shared_ptr<dbindex::LMDBIndex> &Index) {
  return DbIndexBackedIndexStorageManager(std::move(GetProjectInfo),
                                          WorkspaceRoot, Index);
}

} // namespace clangd
} // namespace clang
