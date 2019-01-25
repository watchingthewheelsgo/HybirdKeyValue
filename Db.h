#ifndef __DB_H
#define __DB_H

#include <assert.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include "BplusTree.h"
#include "DBInterface.h"
#include "Tool.h"
namespace hybridKV {

class BplusTreeSplit;
class kvObj;
class Config;
class HashTable;
class ThreadPool;
class scanRes;
    
void* schedule(void *);
void* schedulev2(void *);

class DBImpl : public hyDB {
public:
//    friend class ThreadPool;
    typedef BplusTreeSplit* btPointer;

    DBImpl(int size);
    ~DBImpl();
    
    int Get(const std::string& key, std::string* val);
    int Put(const std::string& key, const std::string& val);
    int Delete(const std::string& key);
    int Scan(const std::string& beginKey, int n, std::vector<std::string>& output);
    int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);
    int Update(const std::string& key, const std::string& val);
    // void rdStartSignal();
    // void rdStopSignal();
    // void signalBG();
    // void newRound();
    static void BGWork(void* db);
    // void newRound() {
        
    // }
    void BgInit();
    void waitBGWork();

    void flush(int i) {
        auto cmd = new cmdInfo();
        cmd->type = kFlushType;
        bt_grp[i]->mutPush(cmd);
    }
    bool finished() {
        for (int i=0; i<bt_size; ++i)
            while (!bt_grp[i]->finished());
    }
    void flushall() {
        for (int i=0; i<bt_size; ++i)
            flush(i);
    }
    BplusTreeSplit* tree(int i) {
        return bt_grp[i];
    }
    void progress() {
        for (int i=0; i<bt_size; ++i) {
            printf("tree %d execs %d cmds.\n", i, tree(i)->writeCnt);
        }
    }
    int Recover();
    int Close();
    int getApproIndex();
private:
    // void queue_push();
    void mergeScan(std::vector<scanRes*>& result, std::vector<std::string>& output);
    Config* cfg;
    uint32_t bt_size;
    BplusTreeSplit** bt_grp;
    HashTable* ht_;
    ThreadPool* thrds;
    int nextIdx;

};

}
#endif
