/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/Protocol.h>
#include <wdt/WdtConfig.h>
#include <wdt/Writer.h>
#include <wdt/util/FileCreator.h>

namespace facebook {
namespace wdt {

class FileWriter : public Writer {
 public:
  FileWriter(ThreadCtx &threadCtx, BlockDetails const *blockDetails,
             FileCreator *fileCreator)
      : threadCtx_(threadCtx),
        blockDetails_(blockDetails),
#ifdef HAS_SYNC_FILE_RANGE
        nextSyncOffset_(blockDetails->offset),
#endif
        fileCreator_(fileCreator) {
  }

  ~FileWriter() override;

  /// @see Writer.h
  ErrorCode open() override;

  /// @see Writer.h
  ErrorCode write(char *buf, int64_t size) override;

  /// @see Writer.h
  int64_t getTotalWritten() override {
    return totalWritten_;
  }

  /// @see Writer.h
  /// This method calls fsync() and posix_fadvise, except if options are set
  /// to disable it.
  ErrorCode sync() override;

  /// @see Writer.h
  ErrorCode close() override;

 private:
  /**
   * calls sync_file_range at disk_sync_interval_mb intervals.
   *
   * @param written   number of bytes last written
   * @param forced    whether to force syncing or not
   */
  bool syncFileRange(int64_t written, bool forced);

  /**
   * Return true if the file is already closed.
   */
  bool isClosed();

  ThreadCtx &threadCtx_;

  /// file handler
  int fd_{-1};

  /// details of the block
  BlockDetails const *blockDetails_;

  /// number of bytes written
  int64_t totalWritten_{0};

#ifdef HAS_SYNC_FILE_RANGE
  /// offset to use for next sync
  int64_t nextSyncOffset_;
  /// number of bytes written since last sync
  int64_t writtenSinceLastSync_{0};
#endif
  /// reference to file creator
  FileCreator *fileCreator_;
};
}
}
