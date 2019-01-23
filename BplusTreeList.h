//
//  BplusTree.h
//  devl
//
//  Created by 吴海源 on 2018/11/14.
//  Copyright © 2018 why. All rights reserved.
//
// data and leaf are allocated in PM
// innernode and leafnode(for index management) are allocated in PM
// keys are sorted inside innernode, not sorted in leafnode. so when splitting leafnode, sort first then partition
// as for innernode, tranverse children node to "jump".

#ifndef BplusTreeList_h
#define BplusTreeList_h

#include <string>
#include <memory>
#include <list>
#include <algorithm>
#include <cstring>
#include <cassert>
#include <deque>
#include <unordered_map>

#include "atomic_pointer.h"
#include "DBInterface.h"
#include "Tool.h"
#include "Configure.h"
#include "kvObject.h"
#include "hyKV_def.h"
#include "latency.hpp"
#include "Thread.h"

namespace std {
    using namespace hybridKV;
    // template specialization hash<kvObj*> and equal_to<kvObj*>, then offer them to unordered_map
    template <>
    class hash<kvObj*> {
    public:
        size_t operator()(const kvObj* k) const {
            return smHasher(k->data(), k->size());
        }
    };
    template <>
    class equal_to<kvObj*> {
    public:
        bool operator()(const kvObj* x, const kvObj* y) const {
            return std::strcmp(x->data(), y->data()) == 0;
        }
    };
}
namespace hybridKV {

static const int INNER_KEYS_L = 4;
static const int INNER_KEYS_MIDPOINT_L = (INNER_KEYS_L / 2);
static const int INNER_KEYS_UPPER_L = INNER_KEYS_MIDPOINT_L + 1;
static const int LEAF_KEYS_L = 48;
static const int LEAF_KEYS_MIDPOINT_L = LEAF_KEYS_L / 2;
static const int LEAF_KEYS_UPPER_L = LEAF_KEYS_MIDPOINT_L + 1;

// persistent
class KVPairFin {
    public:
        KVPairFin(kvObj* key, kvObj* val):hash_(0), key_(key), val_(val){
// #ifdef PM_WRITE_LATENCY_TEST
//             pflush((uint64_t*)(this), sizeof(KVPairFin));
// #endif
        }
        ~KVPairFin() {}
        uint8_t hash() { return hash_; }
        const char* key() const { return key_->data(); }
        size_t keysize() const { return key_->size(); }
        size_t valsize() const { return val_->size(); }
//        const char* key() { return kv; }
        kvObj* getVal() {
            return val_;
        }
        void* freeKey() {
            delete key_;
        }
        const char* val() { return val_->data(); }
        bool valid() { return hash_ != 0; }
        // void clear();
        // void Set(const uint8_t hash, const char* key, const char* value);
        void Update(kvObj* val) {
            val_ = val;
        }
        void setHash(uint8_t hash) {
            hash_ = hash;
        }
    private:
        uint8_t hash_;
        // uint32_t ksize_;
        // uint32_t vsize_;
        kvObj* key_;
        kvObj* val_;
}; 

// persistent
struct KVLeafFin {

    std::list<KVPairFin*> lst;
    // std::unique_ptr<KVPair> slots[LEAF_KEYS];
    KVLeafFin* next;
};

// volatile
struct KVInnerNodeFin;
struct KVNodeFin {
    bool isLeaf = false;
    KVInnerNodeFin* parent;
    virtual ~KVNodeFin() { }
};
// volatile
struct KVInnerNodeFin : KVNodeFin {
    KVInnerNodeFin():keyCount(0), keys(INNER_KEYS_L + 1) { }
    uint8_t keyCount;
    // std::string keys[INNER_KEYS_L + 1];
    std::vector<std::string> keys;
    std::unique_ptr<KVNodeFin> children[INNER_KEYS_L + 2];
};
// volatile
struct KVCachedNodeFin {
    typedef std::list<KVPairFin*>::iterator ITOR;
    KVCachedNodeFin(uint8_t h, std::string&& str, ITOR p):hash(h), key(str), ptr(p) {} 
    uint8_t hash;
    std::string key;
    std::list<KVPairFin*>::iterator ptr;
};
struct KVLeafNodeFin : KVNodeFin {
    // uint8_t hashes[LEAF_KEYS];
    // std::string keys[LEAF_KEYS];
    KVLeafNodeFin():cnt(0) {
    }
    uint8_t cnt;
    std::list<KVCachedNodeFin> lst;
    std::shared_ptr<KVLeafFin> leaf;
    KVLeafNodeFin* next;

};

//#ifdef HiKV_TEST
//typedef leveldb::port::AtomicPointer AtomicPointer;
//class scanResHiKV {
//public:
//    explicit scanResHiKV() {
//        done.Release_Store(reinterpret_cast<void*>(0));
//        elems.clear();
//    }
//    AtomicPointer done;
//    std::list<std::string> elems;
//};
//#endif

// int BplusTreeList::WriteBatch(std::vector<btCmdNode>& batch) {
//     if (batch.empty())
//         return -1;
//     for (auto &cmd:batch) {
//         if (cmd.cmdType == )
//     }
// }

struct btCmdNode {
    cmdType type;
    void* key;
    void* val;
    void* ptr;
};

class BplusTreeList {
public:
    typedef std::deque<btCmdNode*>::reference QREF;
    BplusTreeList(int idx):index(idx), logCmpCnt(0), notFoundCnt(0), skewLeafCnt(0), pushBackCnt(0), dupKeyCnt(0), leafSplitCmp(0), leafCmpCnt(0), leafSplitCnt(0), innerUpdateCnt(0), leafCnt(0), leafSearchCnt(0), depth(0) {};
    ~BplusTreeList(){};

    int WriteBatch(std::vector<btCmdNode>& batch);

    int Insert(KVPairFin* kv);
    int Delete(const std::string& key);

    int Update(KVPairFin* kv, kvObj* newVal) {
        kv->Update(newVal);
    }
    int Delete(const kvObj* key) ;
    int Get(const std::string& key, std::string* val);

    int Scan(const std::string& beginKey, int n, std::vector<std::string>& output);
    int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);

    int Scan(const kvObj* beginKey, int n, scanRes* r);
    int Scan(const kvObj* beginKey, const kvObj* lastKey, scanRes* r);
    // int Update(const std::string& key, const std::string& val); // update is in Insert();
    
    // Interfaces used to handle banckground request.
    void cmd_push(btCmdNode* tdNode) {

        mtx_.Lock();
        auto res = mp.find((kvObj*)tdNode->key);
        if (res!= mp.end()) {
            res->second->val = tdNode->val;
        } else {
            cmdQue.push_back(tdNode);
            mp.insert({(kvObj*)tdNode->key, cmdQue.back()});
        }
        mtx_.unLock();
    }
    // only be called when cmdQue is not empty
    btCmdNode* extractCmd() {
        mtx_.Lock();
        btCmdNode* cmd = cmdQue.front();
        mp.erase((kvObj*)cmd->key);
        cmdQue.pop_front();
        mtx_.unLock();
        return cmd;
    }
    void Geo();
    bool sortedLeaf();
    void printLeafnode(const std::string& key);
    void printNoLeaf(int x);
    int busyCnt() {
        return cmdQue.size();
    }
    bool emptyQue() {
        return cmdQue.empty();
    }
    void setZero() {
        leafSplitCmp = 0;
        leafCmpCnt = 0;
        leafSearchCnt = 0;
        leafCnt = 0;
        leafSplitCnt = 0;
        innerUpdateCnt = 0;
        depth = 0;
        dupKeyCnt = 0;
        skewLeafCnt = 0;
        pushBackCnt = 0;
        logCmpCnt = 0;
        notFoundCnt=0;
        tmr_cache.setZero();
        tmr_insert.setZero();
        tmr_search.setZero();
        tmr_inner.setZero();
        tmr_split.setZero();
    }
    // Test use.
    int leafSplitCmp;
    int leafCmpCnt;
    int leafSearchCnt;
    int leafCnt;
    int leafSplitCnt;
    int innerUpdateCnt;
    int depth;
    int dupKeyCnt;
    int skewLeafCnt;
    int pushBackCnt;
    int logCmpCnt;
    int notFoundCnt;
    TimerRDT tmr_cache, tmr_insert, tmr_search, tmr_inner, tmr_split, tmr_log;
protected:
    // inside methods including "search leafnode" / "leaf split" / "leaf insert" / "recursively inner node update".
    KVLeafNodeFin* leafSearch(const char* key);
    uint8_t PearsonHash(const char* data, const size_t size); // used to speed up compare when "Get()".
    bool leafFillSlotForKey(KVLeafNodeFin* leafnode, const uint8_t hash, KVPairFin* kv);
    void leafSplitFull(KVLeafNodeFin* leafnode, KVPairFin* kv);
    void innerUpdateAfterSplit(KVNodeFin* node, std::unique_ptr<KVNodeFin> new_node, std::string* split_key);

    
private:
    uint8_t index; // sequence number of this Bplus Tree.
    Mutex mtx_;
    // bool toFlush;
    // TimerRDT tmr, tmr_cp;
    std::deque<btCmdNode*> cmdQue;
    std::unordered_map<kvObj*, QREF> mp;  
    std::unique_ptr<KVNodeFin> root; // root node of the B+tree
    std::shared_ptr<KVLeafFin> head; // list head of leaf. All leafs are in one list. 
    // std::shared_ptr<KVLeafNodeFin> hnode; // list head of leaf. All leafs are in one list. 
};
 
}

#endif /* BplusTree_h */
