#include "rocksdb_wrapper.h"

using namespace std;

namespace rdb {
int RocksdbWrapper::do_add_column(const char* name){
    // create column family
    ColumnFamilyOptions c_option{};
    ColumnFamilyHandle* cf;
    s = db->CreateColumnFamily(c_option, name, &cf);
    if(!s.ok()){
        return -1;
    }

    handles.push_back(cf);
    insert_into_map(col_name_to_id_,cf->GetName(),cf->GetID());

    return cf->GetID();
}

} //namespace rdb