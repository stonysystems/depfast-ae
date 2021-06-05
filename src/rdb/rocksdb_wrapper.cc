#include "rocksdb_wrapper.h"

#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <mutex>

namespace rdb {

static RocksdbWrapper* rocksdbwrapper;
static std::once_flag oc;

RocksdbWrapper* rocksdb_wrapper() {
  std::call_once(oc,
                 [] { rocksdbwrapper = new RocksdbWrapper(default_db_path); });

  return rocksdbwrapper;
}

void rocksdb_wrapper_delete() {
  if (rocksdbwrapper != nullptr) {
    delete rocksdbwrapper;
    rocksdbwrapper = nullptr;
  }
}

static bool Mkdirs(const char* dir) {
  char path_buf[1024];
  const int dir_mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  char* p = NULL;
  size_t len = 0;
  snprintf(path_buf, sizeof(path_buf), "%s", dir);
  len = strlen(path_buf);
  if (path_buf[len - 1] == '/') {
    path_buf[len - 1] = 0;
  }
  for (p = path_buf + 1; *p; p++) {
    if (*p == '/') {
      *p = 0;
      int status = mkdir(path_buf, dir_mode);
      if (status != 0 && errno != EEXIST) {
        return false;
      }
      *p = '/';
    }
  }
  int status = mkdir(path_buf, dir_mode);
  if (status != 0 && errno != EEXIST) {
    return false;
  }
  return true;
}

const std::string RocksdbWrapper::anonymous_user = "";

RocksdbWrapper::RocksdbWrapper(const std::string& data_dir)
    : data_dir_(data_dir) {
  bool ok = Mkdirs(data_dir.c_str());
  assert(ok == true);
  // Create default database for shared namespace, i.e. anonymous user
  std::string full_name = data_dir + "/@db";

  Options options;
  // Optimize RocksDB.
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  // This is a new database.
  options.create_if_missing = true;
  DB* default_db = NULL;
  Status status = DB::Open(options, full_name, &default_db);
  assert(status.ok());
  dbs_[""] = default_db;
}

RocksdbWrapper::~RocksdbWrapper() {
  MutexLock lock(&mu_);
  for (std::map<std::string, DB*>::iterator it = dbs_.begin(); it != dbs_.end();
       ++it) {
    delete it->second;
    it->second = NULL;
  }
}

bool RocksdbWrapper::OpenDatabase(const std::string& name) {
  {
    MutexLock lock(&mu_);
    if (dbs_.find(name) != dbs_.end()) {
      return true;
    }
  }

  std::string full_name = data_dir_ + "/" + name + "@db";
  Options options;
  // Optimize RocksDB.
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  // This is a new database.
  options.create_if_missing = true;
  DB* current_db = NULL;
  Status status = DB::Open(options, full_name, &current_db);

  assert(status.ok());
  {
    MutexLock lock(&mu_);
    dbs_[name] = current_db;
  }
  return status.ok();
}

void RocksdbWrapper::CloseDatabase(const std::string& name) {
  MutexLock lock(&mu_);
  std::map<std::string, DB*>::iterator dbs_it = dbs_.find(name);
  if (dbs_it == dbs_.end()) {
    return;
  }
  delete dbs_it->second;
  dbs_it->second = NULL;
  dbs_.erase(dbs_it);
}

bool RocksdbWrapper::Get(const std::string& name, const std::string& key,
                         std::string* value) {
  if (value == NULL) {
    return false;
  }
  DB* db_ptr = NULL;
  {
    MutexLock lock(&mu_);
    if (dbs_.find(name) == dbs_.end()) {
      return false;
    }
    db_ptr = dbs_[name];
    if (db_ptr == NULL) {
      return false;
    }
  }
  Status status = db_ptr->Get(ReadOptions(), key, value);
  return (status.ok());
}

bool RocksdbWrapper::Put(const std::string& name, const std::string& key,
                         const std::string& value) {
  DB* db_ptr = NULL;
  {
    MutexLock lock(&mu_);
    if (dbs_.find(name) == dbs_.end()) {
      return false;
    }
    db_ptr = dbs_[name];
    if (db_ptr == NULL) {
      return false;
    }
  }
  Status status = db_ptr->Put(WriteOptions(), key, value);
  return (status.ok());
}

bool RocksdbWrapper::Put(const std::string& name, const std::string& key,
                         const Slice& value) {
  DB* db_ptr = NULL;
  {
    MutexLock lock(&mu_);
    if (dbs_.find(name) == dbs_.end()) {
      return false;
    }
    db_ptr = dbs_[name];
    if (db_ptr == NULL) {
      return false;
    }
  }
  Status status = db_ptr->Put(WriteOptions(), key, value);
  return (status.ok());
}

bool RocksdbWrapper::Delete(const std::string& name, const std::string& key) {
  DB* db_ptr = NULL;
  {
    MutexLock lock(&mu_);
    if (dbs_.find(name) == dbs_.end()) {
      return false;
    }
    db_ptr = dbs_[name];
    if (db_ptr == NULL) {
      return false;
    }
  }
  Status status = db_ptr->Delete(WriteOptions(), key);

  return (status.ok());
}

std::string RocksdbWrapper::Iterator::key() const {
  return (it_ != NULL) ? it_->key().ToString() : "";
}

std::string RocksdbWrapper::Iterator::value() const {
  return (it_ != NULL) ? it_->value().ToString() : "";
}

RocksdbWrapper::Iterator* RocksdbWrapper::Iterator::Seek(std::string key) {
  if (it_ != NULL) {
    it_->Seek(key);
  }
  return this;
}

RocksdbWrapper::Iterator* RocksdbWrapper::Iterator::Next() {
  if (it_ != NULL) {
    it_->Next();
  }
  return this;
}

bool RocksdbWrapper::Iterator::Valid() const {
  return (it_ != NULL) ? it_->Valid() : false;
}

Status RocksdbWrapper::Iterator::status() const { return it_->status(); }

RocksdbWrapper::Iterator* RocksdbWrapper::NewIterator(const std::string& name) {
  DB* db_ptr = NULL;
  {
    MutexLock lock(&mu_);
    std::map<std::string, DB*>::iterator dbs_it = dbs_.find(name);
    if (dbs_it == dbs_.end()) {
      return NULL;
    }
    db_ptr = dbs_it->second;
    if (db_ptr == NULL) {
      return NULL;
    }
  }

  return new RocksdbWrapper::Iterator(db_ptr, ReadOptions());
}

}  // namespace rdb
