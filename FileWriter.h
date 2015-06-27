#pragma once

#include <wdt/WdtConfig.h>
#include "Writer.h"
#include "FileCreator.h"
#include "Protocol.h"

namespace facebook {
namespace wdt {

class FileWriter : public Writer {
 public:
  FileWriter(int threadIndex, BlockDetails const *blockDetails,
             FileCreator *fileCreator)
      : threadIndex_(threadIndex),
        blockDetails_(blockDetails),
#ifdef HAS_SYNC_FILE_RANGE
        nextSyncOffset_(blockDetails->offset),
#endif
        fileCreator_(fileCreator) {
  }

  virtual ~FileWriter() {
    close();
  }

  /// @see Writer.h
  virtual ErrorCode open() override;

  /// @see Writer.h
  virtual ErrorCode write(char *buf, int64_t size) override;

  /// @see Writer.h
  virtual int64_t getTotalWritten() override {
    return totalWritten_;
  }

  /// @see Writer.h
  virtual void close() override;

 private:
  /**
   * calls sync_file_range at disk_sync_interval_mb intervals.
   *
   * @param written   number of bytes last written
   * @param forced    whether to force syncing or not
   */
  void syncFileRange(int64_t written, bool forced);

  /// file handler
  int fd_{-1};
  /// index of the owner receiver thread. This is needed for co-ordination of
  /// disk space allocation in fileCreator
  int threadIndex_;

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
