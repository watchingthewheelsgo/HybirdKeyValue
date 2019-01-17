#ifndef __SKIPLIST_H
#define __SKIPLIST_H

// #include <>
#include <cstdlib>
#include <list>
#include <vector>
#include <deque>
#include "kvObject.h"
#include "Thread.h"
#include "Tool.h"
#include "Random.h"
#include "latency.hpp"
//#include "atomic_pointer.h"

namespace hybridKV {


struct slCmdNode {
	void* priv;
    cmdType type;
    void* key;
    void* val;
};


//////////////////////////////////////////////////////////////////////
//
// An implementation of SkipList, including Insert, Del, Update, Scan etc.
//
////////////////////////////////////////////////////////////////////////
class SkipList {
 public:
    struct Node {
        
        Node(void* key, void* val, int hgt) : key_(key), val_(val), height(hgt) { }
        
        Node* Next(int n){
            return reinterpret_cast<Node *>(forward[n]);
        }
        uint64_t* levelAddr(int n) {
            return (uint64_t*)(&forward[n]);
        }
        void setNext(int i, Node* next, bool nvm_allocated) {
            forward[i] = next;
#ifdef PM_WRITE_LATENCY_TEST
            if (nvm_allocated)
                pflush((uint64_t*)(&forward[i]),sizeof(Node*));
#endif
        }
        bool Valid() {
            return key_ != nullptr;
        }
        void* key_;
        void* val_;
        int height;
        void* cached_key;
        Node* forward[1];
        // Mutex part should be added when performing multi-thread
        // Mutex mutex;
    };

 public:
    explicit SkipList(int idx);
    
    int getMaxHght() { return max_height; }

    Node* newNode(void* key, void* val, bool normal);

    bool keyIsAfter(void* tarKey, Node* now);
    Node* findCeilNode(void* key, Node** prev);
    Node* findCeilNodeRd(void* key, Node** prev);
    //Methods
//    int Scan(Node* tar, int n, std::vector<std::string*>& res);
    
    int GetVal(const std::string& key, std::string* value);
//    int Insert(void* key, void* val, int* height);
    int Insert(Node* cmdNode);
    int Scan(const std::string& startkey, int n, std::vector<std::string>& val);
    int Scan(void *startkey, void* limit, scanRes &res);
    // int Insert(void *key, void *slNodeAddr);
    int Delete(const std::string& keyStr);
    int Update(const std::string& key, const std::string& val);

    bool emptyQue() {
        return cmdQue.empty();
    }

    // void lockQue() {
    //     mtx_.Lock();
    // }
    // void unlockQue() {
    //     mtx_.unlock();
    // }
    void cmd_push(void *tdNode) {
        mtx_.Lock();
        cmdQue.push_back((slCmdNode*)tdNode);
        mtx_.unLock();
    }
    // only be called when cmdQue is not empty
    slCmdNode* extractCmd() {
        mtx_.Lock();
        slCmdNode* cmd = cmdQue.front();
        cmdQue.pop_front();
        mtx_.unLock();
        return cmd;
    }

    void finAndStop() {
        bgthread_started = false;
    }
    void start_bgthread() {
        if (bgthread_started == false)
            bgthread_started = true;
    }
    bool onSchedule() {
        return bgthread_started;
    }
    int idx() { return idx_; }
    void waitForBGWork() {
        while (!cmdQue.empty());
    }
    void scanAll();
    void setCmpCntZero() {
        cmpCnt = 0;
        maxCmpCnt = 0;
    }
    uint32_t cmpCnt;
    int maxCmpCnt;
private:
	Random rnd_;
    unsigned int randomHeight() {
        int level = 1;
        unsigned int prob = 3;
        while (level < limit_height && ((random() & 0xffff) < (0xffff / prob)))
            level++;
        
        return level;
    }
    
    enum { limit_height = 32 };
    unsigned int max_height;
    Node* const head_;
    // Reserverd for Multi sl
    int minKeyLen;
    int maxKeyLen;
    // unsigned int seed_;
    Mutex mtx_;
    std::deque<slCmdNode*> cmdQue;
    bool bgthread_started;
    int idx_;
};


}

#endif
