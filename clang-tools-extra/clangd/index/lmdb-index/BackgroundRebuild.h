//===--- BackgroundIndexRebuild.h - when to rebuild the bg index--*- C++-*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
//
// This file contains an implementation detail of the background indexer
// (Background.h), which is exposed for testing.
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_BACKGROUNDREBUILD_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_BACKGROUNDREBUILD_H

#include "DocumentStore.h"
#include "index/FileIndex.h"
#include "index/Index.h"
#include "support/Threading.h"
#include <cstddef>
#include <utility>
#include <vector>

namespace clang {
namespace clangd {
namespace LMDBBackground {

struct ShardQueueItem {
  llvm::sys::fs::TempFile ShardTmpFile;
  int64_t CreationTime;
  std::string Path;
  std::string MainPath;

  ~ShardQueueItem() { (void)ShardTmpFile.discard(); }
  ShardQueueItem(ShardQueueItem &&) = default;
  ShardQueueItem &operator=(ShardQueueItem &&) = default;
};

// The BackgroundIndexRebuilder builds the serving data structures periodically
// in response to events in the background indexer. The goal is to ensure the
// served data stays fairly fresh, without wasting lots of CPU rebuilding it
// often.
//
// The index is always built after a set of shards are loaded from disk.
// This happens when clangd discovers a compilation database that we've
// previously built an index for. It's a fairly fast process that yields lots
// of data, so we wait to get all of it.
//
// The index is built after indexing a few translation units, if it wasn't built
// already. This ensures quick startup if there's no existing index.
// Waiting for a few random TUs yields coverage of the most common headers.
//
// The index is rebuilt every N TUs, to keep if fresh as files are indexed.
//
// The index is rebuilt every time the queue goes idle, if it's stale.
//
// All methods are threadsafe. They're called after FileSymbols is updated
// etc. Without external locking, the rebuilt index may include more updates
// than intended, which is fine.
//
// This class is exposed in the header so it can be tested.
class BackgroundIndexRebuilder {
public:
  BackgroundIndexRebuilder(SwapIndex *Target,
                           std::shared_ptr<lmdb_index::DocumentStore> Source,
                           unsigned Threads);
  ~BackgroundIndexRebuilder() { shutdown(); };

  // Queue up a TU to be indexed.
  void queueTUShard(ShardQueueItem &&Item);

  // Called to indicate a TU has been indexed.
  // May rebuild, if enough TUs have been indexed.
  void indexedTU();
  // Called to indicate that all worker threads are idle.
  // May reindex, if the index is not up to date.
  void idle();
  // Called to indicate we're going to load a batch of shards from disk.
  // startLoading() and doneLoading() must be paired, but multiple loading
  // sessions may happen concurrently.
  void startLoading();
  // Called to indicate some shards were actually loaded from disk.
  void loadedShard(size_t ShardCount);
  // Called to indicate we're finished loading shards from disk.
  // May rebuild (if any were loaded).
  void doneLoading();

  // Ensures we won't start any more rebuilds.
  void shutdown();

  // Thresholds for rebuilding as TUs get indexed. Exposed for testing.
  const unsigned TUsBeforeFirstBuild; // Typically one per worker thread.
  const unsigned TUsBeforeRebuild = 100;

private:
  // Worker proc
  void work();
  // Notify the worker to do work
  void notifyWorkerLocked();
  // Refresh Target SwapIndex
  void refreshIndexLocked(uint64_t Version);

  // Run Check under the lock, and rebuild if it returns true.
  void maybeRebuild(const char *Reason, std::function<bool()> Check);
  bool enoughTUsToRebuild() const;

  // All transient state is guarded by the mutex.
  std::mutex Mu;
  bool ShouldStop = false;
  // Index builds are versioned. ActiveVersion chases StartedVersion.
  uint64_t StartedVersion = 0;
  uint64_t ActiveVersion = 0;
  // How many TUs have we indexed so far since startup?
  uint64_t IndexedTUs = 0;
  uint64_t IndexedTUsAtLastRebuild = 0;
  // Are we loading shards? May be multiple concurrent sessions.
  uint64_t Loading = 0;
  uint64_t LoadedShards; // In the current loading session.
  // Worker notification
  bool WorkerNotify = false;
  std::condition_variable WorkerNotifyCV;

  SwapIndex *Target;
  std::shared_ptr<lmdb_index::DocumentStore> Source;

  AsyncTaskRunner ThreadPool;
  std::vector<ShardQueueItem> QueuedTUShards;
};

} // namespace LMDBBackground
} // namespace clangd
} // namespace clang

#endif
