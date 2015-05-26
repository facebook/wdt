#pragma once

#include "Writer.h"
#include "FileCreator.h"

namespace facebook {
namespace wdt {
class FileWriter : public Writer {
 public:
  FileWriter(int threadIndex, std::string &fileName, uint64_t seqId,
             uint64_t fileSize, uint64_t offset, uint64_t dataSize,
             FileCreator *fileCreator)
      : threadIndex_(threadIndex),
        fileName_(fileName),
        seqId_(seqId),
        fileSize_(fileSize),
        offset_(offset),
        dataSize_(dataSize),
        nextSyncOffset_(offset),
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
  virtual uint64_t getTotalWritten() override {
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
  void syncFileRange(uint64_t written, bool forced);

  /// file handler
  int fd_{-1};
  /// index of the owner receiver thread. This is needed for co-ordination of
  /// disk space allocation in fileCreator
  int threadIndex_;
  /// name of the file
  std::string fileName_;
  /// sequence-id of the file
  uint64_t seqId_;
  /// size of the file
  uint64_t fileSize_;
  /// offset of the block from the start of the file
  uint64_t offset_;
  /// size of the block
  uint64_t dataSize_;

  /// number of bytes written
  uint64_t totalWritten_{0};

  /// offset to use for next sync
  uint64_t nextSyncOffset_;
  /// number of bytes written since last sync
  uint64_t writtenSinceLastSync_{0};
  /// reference to file creator
  FileCreator *fileCreator_;
};
}
}
