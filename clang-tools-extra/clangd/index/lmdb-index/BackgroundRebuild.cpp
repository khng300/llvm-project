//===-- BackgroundRebuild.cpp - when to rebuild thei background index -----===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "BackgroundRebuild.h"
#include "LMDBWrapperError.h"
#include "index/FileIndex.h"
#include "support/Logger.h"
#include "support/Trace.h"
#include "llvm/Support/FileSystem.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <numeric>
#include <queue>
#include <random>
#include <string>
#include <thread>

namespace clang {
namespace clangd {
namespace LMDBBackground {

namespace {
std::unique_ptr<IndexFileIn> loadShard(llvm::sys::fs::TempFile &ShardTmpFile) {
  auto Buffer = llvm::MemoryBuffer::getOpenFile(
      ShardTmpFile.FD, ShardTmpFile.TmpName, uint64_t(-1), false);
  if (!Buffer)
    return nullptr;
  if (auto I =
          readIndexFile(Buffer->get()->getBuffer(), SymbolOrigin::Background))
    return std::make_unique<IndexFileIn>(std::move(*I));
  else
    elog("Error while reading shard {0}: {1}", ShardTmpFile.TmpName,
         I.takeError());
  return nullptr;
}
} // namespace

BackgroundIndexRebuilder::BackgroundIndexRebuilder(
    SwapIndex *Target, std::shared_ptr<lmdb_index::DocumentStore> Source,
    unsigned Threads)
    : TUsBeforeFirstBuild(Threads), Target(Target), Source(Source) {
  ThreadPool.runAsync("background-index-rebuilder", [this]() { work(); });
}

void BackgroundIndexRebuilder::queueTUShard(ShardQueueItem &&QI) {
  std::lock_guard<std::mutex> Lock(Mu);
  QueuedTUShards.emplace_back(std::move(QI));
}

bool BackgroundIndexRebuilder::enoughTUsToRebuild() const {
  if (!ActiveVersion)                         // never built
    return IndexedTUs == TUsBeforeFirstBuild; // use low threshold
  // rebuild if we've reached the (higher) threshold
  return IndexedTUs >= IndexedTUsAtLastRebuild + TUsBeforeRebuild;
}

void BackgroundIndexRebuilder::indexedTU() {
  maybeRebuild("after indexing enough files", [this] {
    ++IndexedTUs;
    if (Loading)
      return false;                      // rebuild once loading finishes
    if (ActiveVersion != StartedVersion) // currently building
      return false;                      // no urgency, avoid overlapping builds
    return enoughTUsToRebuild();
  });
}

void BackgroundIndexRebuilder::idle() {
  maybeRebuild("when background indexer is idle", [this] {
    // rebuild if there's anything new in the index.
    // (even if currently rebuilding! this ensures eventual completeness)
    return IndexedTUs > IndexedTUsAtLastRebuild;
  });
}

void BackgroundIndexRebuilder::startLoading() {
  std::lock_guard<std::mutex> Lock(Mu);
  if (!Loading)
    LoadedShards = 0;
  ++Loading;
}
void BackgroundIndexRebuilder::loadedShard(size_t ShardCount) {
  std::lock_guard<std::mutex> Lock(Mu);
  assert(Loading);
  LoadedShards += ShardCount;
}
void BackgroundIndexRebuilder::doneLoading() {
  maybeRebuild("after loading index from disk", [this] {
    assert(Loading);
    --Loading;
    if (Loading)    // was loading multiple batches concurrently
      return false; // rebuild once the last batch is done.
    // Rebuild if we loaded any shards, or if we stopped an indexedTU rebuild.
    return LoadedShards > 0 || enoughTUsToRebuild();
  });

  std::lock_guard<std::mutex> Lock(Mu);
  auto BuildVersion = ++StartedVersion;
  refreshIndexLocked(BuildVersion);
}

void BackgroundIndexRebuilder::shutdown() {
  std::lock_guard<std::mutex> Lock(Mu);
  ShouldStop = true;
  notifyWorkerLocked();
  ThreadPool.wait();
}

void BackgroundIndexRebuilder::maybeRebuild(const char *,
                                            std::function<bool()> Check) {
  std::unique_lock<std::mutex> Lock(Mu);
  if (!ShouldStop && Check())
    notifyWorkerLocked();
}

void BackgroundIndexRebuilder::work() {
  while (true) {
    uint64_t BuildVersion;
    decltype(QueuedTUShards) FilesToUpdate;
    {
      std::unique_lock<std::mutex> Lock(Mu);
      if (QueuedTUShards.empty())
        WorkerNotifyCV.wait(Lock, [this]() { return WorkerNotify; });
      WorkerNotify = false;
      if (ShouldStop)
        break;
      BuildVersion = ++StartedVersion;
      IndexedTUsAtLastRebuild = IndexedTUs;
      FilesToUpdate = std::move(QueuedTUShards);
      QueuedTUShards.clear();
    }

    if (!FilesToUpdate.empty()) {
      std::error_code CommitEC;
      size_t OldMapSizeHint = 0;
      do {
        CommitEC = [&]() {
          auto TxnOrErr = Source->beginTransaction(false, "<Build index>");
          if (!TxnOrErr)
            return TxnOrErr.getError();
          OldMapSizeHint = TxnOrErr->get()->databaseMapSize();
          auto &Txn = *TxnOrErr->get();
          auto StartTime = std::chrono::system_clock::now();
          {

            // Build and store new slabs for each updated file.
            for (auto &QI : FilesToUpdate) {
              auto IF = loadShard(QI.ShardTmpFile);
              if (!IF)
                continue;

              // Only store command line hash for main files of the TU, since
              // our current model keeps only one version of a header file.
              if (QI.Path != QI.MainPath)
                IF->Cmd.reset();

              // We need to store shards before updating the index, since the
              // latter consumes slabs.
              // FIXME: Also skip serializing the shard if it is already
              // up-to-date.
              IndexFileOut IOut(*IF);
              if (auto Error = Source->updateShard(Txn, QI.Path, &IOut,
                                                   QI.CreationTime)) {
                return Error;
              }
            }
            auto EC = Txn.updateIndexedSymbols();
            if (EC)
              return EC;
          }
          auto EndTime = std::chrono::system_clock::now();
          log("maybeRebuild(): SymbolIndexTime: {0}", EndTime - StartTime);

          return std::move(Txn).commit();
        }();
        if (CommitEC) {
          if (CommitEC != lmdb_index::DSError::DatabaseMapFull) {
            elog("maybeRebuild error: {0}", CommitEC.message());
            llvm::report_fatal_error("maybeRebuild failed");
          }
          auto EC = Source->growDatabaseMapSize(OldMapSizeHint);
          if (EC) {
            elog("Instance->growSize() error: {0}", EC.message());
            llvm::report_fatal_error("maybeRebuildfailed");
          }
        }
      } while (CommitEC == lmdb_index::DSError::DatabaseMapFull);
    }

    std::lock_guard<std::mutex> Lock(Mu);
    // Guard against rebuild finishing in the wrong order.
    if (BuildVersion != 0 && BuildVersion > ActiveVersion)
      refreshIndexLocked(BuildVersion);
  }
}

void BackgroundIndexRebuilder::notifyWorkerLocked() {
  if (!WorkerNotify) {
    WorkerNotify = true;
    WorkerNotifyCV.notify_all();
  }
}

void BackgroundIndexRebuilder::refreshIndexLocked(uint64_t Version) {
  if (Version > ActiveVersion) {
    ActiveVersion = Version;
    std::unique_ptr<SymbolIndex> NewIndex =
        lmdb_index::createSymbolIndex(Source);
    vlog("BackgroundIndex: serving version {0} ({1} bytes)", Version,
         NewIndex->estimateMemoryUsage());
    Target->reset(std::move(NewIndex));
  }
}

} // namespace LMDBBackground
} // namespace clangd
} // namespace clang
