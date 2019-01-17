#ifndef __DB_H
#define __DB_H

#include <assert.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "DBInterface.h"

namespace hybridKV {

class SkipList;
class kvObj;
class Config;
class HashTable;
class ThreadPool;
class scanRes;
    
void* schedule(void *);
    
class DBImpl : public hyDB {
public:
//    friend class ThreadPool;
    typedef SkipList* slPointer;
    
    DBImpl(int size);
    ~DBImpl();
    
    int Get(const std::string& key, std::string* val);
    int Put(const std::string& key, const std::string& val);
    int Delete(const std::string& key);
//    int Scan(const std::string& beginKey, uint64_t n, std::vector<std::string>& output);
    int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);
    int Update(const std::string& key, const std::string& val);
    static void BGWork(void* db);
    void BgInit();
    void waitBGWork();
    void slGeo();
    void showCfg();
    int Recover();
    int Close();
    void debug() { }

private:
    // void queue_push();
    void mergeScan(std::vector<scanRes*>& result, std::vector<std::string>& output);
    Config* cfg;
    uint32_t sl_size;
    SkipList** sl_grp;
    HashTable* ht_;
    ThreadPool* thrds;

};

}
#endif
