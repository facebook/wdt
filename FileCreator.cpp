#include "FileCreator.h"

#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

namespace facebook {
namespace wdt {

using std::string;

int FileCreator::createFile(const string &relPathStr) {
  CHECK(!relPathStr.empty());
  CHECK(relPathStr[0] != '/');
  CHECK(relPathStr.back() != '/');

  std::string path(rootDir_);
  path.append(relPathStr);

  int p = relPathStr.size();
  while (p && relPathStr[p - 1] != '/') {
    --p;
  }
  std::string dir;
  if (p) {
    dir.assign(relPathStr.data(), p);
    if (!createDirRecursively(dir)) {
      // retry with force
      LOG(ERROR) << "failed to create dir " << dir << " recursively, "
                 << "trying to force directory creation";
      if (!createDirRecursively(dir, true /* force */)) {
        LOG(ERROR) << "failed to create dir " << dir << " recursively";
        return -1;
      }
    }
  }
  int res = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (res < 0) {
    if (dir.empty()) {
      PLOG(ERROR) << "failed creating file " << path;
      return -1;
    }
    PLOG(ERROR) << "failed creating file " << path << ", trying to "
                << "force directory creation";
    if (!createDirRecursively(dir, true /* force */)) {
      LOG(ERROR) << "failed to create dir " << dir << " recursively";
      return -1;
    }
    res = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (res < 0) {
      PLOG(ERROR) << "failed creating file " << path;
      return -1;
    }
  }
  LOG(VERBOSE) << "successfully created file " << path;
  return res;
}

bool FileCreator::createDirRecursively(const std::string &dir, bool force) {
  int pos = 0;
  std::string subdir;
  while (pos < dir.size()) {
    while (pos < dir.size() && dir[pos] != '/') {
      subdir.push_back(dir[pos]);
      ++pos;
    }
    CHECK(pos < dir.size());
    subdir.push_back('/');
    ++pos;
    if (force || !dirCreated(subdir)) {
      std::string fullDir(rootDir_ + subdir);
      int code = mkdir(fullDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
      if (code != 0 && errno != EEXIST) {
        PLOG(ERROR) << "failed to make directory " << fullDir;
        return false;
      }
      LOG(INFO) << "made dir " << fullDir;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        createdDirs_.insert(subdir);
      }
    }
  }
  return true;
}

/* static */
void FileCreator::addTrailingSlash(string &path) {
  if (path.back() != '/') {
    path.push_back('/');
    LOG(VERBOSE) << "Added missing trailing / to " << path;
  }
}
}
}
