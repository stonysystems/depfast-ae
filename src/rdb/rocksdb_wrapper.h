#pragma once

#include <assert.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "base/threading.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

namespace rdb {

using namespace ROCKSDB_NAMESPACE;

using namespace rrr;

const std::string default_db_path = "/db";

class RocksdbWrapper {
 public:
  static const std::string anonymous_user;
  static Slice MakeSlice(char* data, size_t n) { return Slice(data, n); }
  RocksdbWrapper(const std::string& data_dir);
  ~RocksdbWrapper();

  bool OpenDatabase(const std::string& name);
  void CloseDatabase(const std::string& name);

  // The name is the different database.
  bool Get(const std::string& name, const std::string& key, std::string* value);
  bool Put(const std::string& name, const std::string& key,
           const std::string& value);
  bool Put(const std::string& name, const std::string& key, const Slice& value);
  bool Delete(const std::string& name, const std::string& key);

  bool Get(const std::string& key, std::string* value) {
    return Get(anonymous_user, key, value);
  }
  bool Put(const std::string& key, const std::string& value) {
    return Put(anonymous_user, key, value);
  }
  bool Put(const std::string& key, const Slice& value) {
    return Put(anonymous_user, key, value);
  }

  bool Delete(const std::string& key) { return Delete(anonymous_user, key); }

 public:
  class Iterator {
   public:
    Iterator() : it_(NULL) {}
    Iterator(DB* db, const ReadOptions& option) {
      it_ = db->NewIterator(option);
    }
    ~Iterator() {
      if (it_ != NULL) {
        delete it_;
        it_ = NULL;
      }
    }

    std::string key() const;
    std::string value() const;

    Iterator* Seek(std::string key);
    Iterator* Next();

    bool Valid() const;
    Status status() const;

   private:
    rocksdb::Iterator* it_;
  };

  Iterator* NewIterator(const std::string& name);

 private:
  Mutex mu_;
  std::string data_dir_;
  std::map<std::string, DB*> dbs_;
};

// Help class for mutex
class MutexLock {
 public:
  explicit MutexLock(Mutex* mu) : mu_(mu) { this->mu_->lock(); }
  // No copying allowed
  MutexLock(const MutexLock&) = delete;
  void operator=(const MutexLock&) = delete;
  ~MutexLock() { this->mu_->unlock(); }

 private:
  Mutex* const mu_;
};

void rocksdb_wrapper_delete();
RocksdbWrapper* rocksdb_wrapper();

}  // namespace rdb
