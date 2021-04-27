#include "rocksdb_wrapper.h"

#include <mutex>
using namespace std;

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

int RocksdbWrapper::do_add_column(const char* name) {
  // create column family
  ColumnFamilyOptions c_option{};
  ColumnFamilyHandle* cf;
  s = db->CreateColumnFamily(c_option, name, &cf);
  if (!s.ok()) {
    return -1;
  }

  handles.push_back(cf);
  insert_into_map(col_name_to_id_, cf->GetName(), cf->GetID());

  return cf->GetID();
}

}  // namespace rdb
