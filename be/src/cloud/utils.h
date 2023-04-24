#pragma once

#include <functional>

#include "cloud/cloud_tablet_mgr.h"
#include "common/status.h"

namespace doris {
class DataDir;

namespace io {
class FileSystem;
} // namespace io

namespace cloud {
class MetaMgr;

// Get global cloud DataDir.
// We use `fs` under cloud DataDir as destination s3 storage for load tasks.
DataDir* cloud_data_dir();

// Get global MetaMgr.
MetaMgr* meta_mgr();

// Get global CloudTabletMgr
CloudTabletMgr* tablet_mgr();

// Get fs with latest object store info.
std::shared_ptr<io::FileSystem> latest_fs();

Status bthread_fork_and_join(const std::vector<std::function<Status()>>& tasks, int concurrency);

} // namespace cloud
} // namespace doris
