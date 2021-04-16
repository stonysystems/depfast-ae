#pragma once

#include <assert.h>

#include <unordered_map>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"



namespace rdb{

using namespace ROCKSDB_NAMESPACE;

using colid_t=int;


const std::string default_db_path="/tmp/rocksdb_example";


class RocksdbWrapper {
    
public:
    //It is the type for store byte stream.
    static Slice MakeSlice(char * data,size_t n){
        return Slice(data,n);
    }

    RocksdbWrapper(std::string path = default_db_path) : db_path(path){

        Options options;
        // Optimize RocksDB.
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present

        // List all column family
        std::vector<std::string> column_families_list;
        s = DB::ListColumnFamilies(options,db_path,&column_families_list);

        if(!s.ok()){
            //This is a new database.
            options.create_if_missing = true;
            s = DB::Open(options, db_path, &db);
            //optional
            assert(s.ok());
            handles.push_back(db->DefaultColumnFamily());
            insert_into_map(col_name_to_id_,db->DefaultColumnFamily()->GetName(),db->DefaultColumnFamily()->GetID());
        }else{
            std::vector<ColumnFamilyDescriptor> column_families;
            for(auto column_name : column_families_list){

                column_families.push_back(ColumnFamilyDescriptor(
            column_name, ColumnFamilyOptions()));

            }
            s = DB::Open(options, db_path, column_families, &handles, &db);
            assert(s.ok());

            for(auto handle:handles){

                insert_into_map(col_name_to_id_,handle->GetName(),handle->GetID());

            }
        }

    }

    ~RocksdbWrapper() { 
          // close db
        for (auto handle : handles) {
            //s = db->DestroyColumnFamilyHandle(handle);
            assert(s.ok());
        }
        delete db; 
    }

    // Add ColumnFamily.
    int add_column(const char* name) {
        return do_add_column(name);
    }

    colid_t get_column_id(std::string& name) {
        auto it = col_name_to_id_.find(name);
        if (it != std::end(col_name_to_id_)) {
            return col_name_to_id_[name];
        }
        return -1;
    }

    ColumnFamilyHandle* get_column_handle(std::string& name) {
        colid_t col_id = get_column_id(name);
        if (col_id < 0) {
            return nullptr;
        } else {
            return handles[col_id];
        }
    }

    ColumnFamilyHandle* get_column_handle(colid_t column_id) {
        return handles[column_id];
    }


    rocksdb::Status GetDbStatus(){

        return s;

    }

    bool SetOption(std::string& key, std::string& value){

        s = db->SetOptions({{key, value}});
        return s.ok();
    
    }

    //GET
    Slice Get(const Slice& key){
      return Get(db->DefaultColumnFamily(),key);
    }

    Slice Get(ColumnFamilyHandle* column_handle, const Slice& key){
        PinnableSlice val;
        s = db->Get(ReadOptions(),column_handle, key, &val);
        if(!s.ok()){
            //give some error info.
        }
        return val;
    }

    //PUT
    bool Put(const Slice& key, Slice& val){
    
        return Put(db->DefaultColumnFamily(),key,val);

    }

    bool Put(ColumnFamilyHandle* column_handle, const Slice& key, Slice& val){
    
        s = db->Put(WriteOptions(), column_handle ,key, val);
        return s.ok();

    }

    //DELETE
    bool Delete(const Slice& key){
        
        return Delete(db->DefaultColumnFamily(),key);

    }

    bool Delete(ColumnFamilyHandle* column_handle, const Slice& key){
    
        s = db->Delete(WriteOptions(), key);
        return s.ok();

    }

    typedef std::vector<ColumnFamilyHandle*>::const_iterator iterator;
    iterator begin() const {
        return std::begin(handles);
    }
    iterator end() const {
        return std::end(handles);
    }
    size_t columns_count() const {
        return handles.size();
    }


protected:

    std::string db_path;
    DB* db;
    Status s;

    std::vector<ColumnFamilyHandle*> handles;
    std::unordered_map<std::string, colid_t> col_name_to_id_;
    

private:

    int do_add_column(const char* name);
    void insert_into_map(std::unordered_map<std::string, colid_t> map,std::string key, colid_t val){
        map.insert(std::make_pair(key,val));
    }
};

} //namespace rdb
