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

#include "atomic_pointer.h"
#include "DBInterface.h"
#include "Tool.h"
#include "Configure.h"
#include "kvObject.h"
#include "hyKV_def.h"
#include "latency.hpp"

namespace hybridKV {

static const int INNER_KEYS_L = 4;
static const int INNER_KEYS_MIDPOINT_L = (INNER_KEYS_L / 2);
static const int INNER_KEYS_UPPER_L = INNER_KEYS_MIDPOINT_L + 1;
static const int LEAF_KEYS_L = 8;
static const int LEAF_KEYS_MIDPOINT_L = LEAF_KEYS_L / 2;
static const int LEAF_KEYS_UPPER_L = LEAF_KEYS_MIDPOINT_L + 1;

// persistent
class KVPairFin {
    public:
        KVPairFin(kvObj* key, kvObj* val):hash_(0), key_(key), val_(val){}
        uint8_t hash() { return hash_; }
        const char* key() const { return key_->data(); }
        size_t keysize() const { return key_->size(); }
        size_t valsize() const { return val_->size(); }
//        const char* key() { return kv; }
        kvObj* getVal() {
            return val_;
        }
        const char* val() { return val_->data(); }
        bool valid() { return hash_ != 0; }
        // void clear();
        // void Set(const uint8_t hash, const char* key, const char* value);
        void Update(KVPairFin* kv) {
            val_ = kv->getVal();
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
    uint8_t keyCount;
    std::string keys[INNER_KEYS_L + 1];
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
#ifdef NEED_SCAN
    KVLeafNodeFin* next;
#endif
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

class BplusTreeList {
public:
    BplusTreeList():dupKeyCnt(0), leafSplitCmp(0), leafCmpCnt(0), leafSplitCnt(0), innerUpdateCnt(0), leafCnt(0), leafSearchCnt(0), depth(0) {};
    ~BplusTreeList(){};
    int Insert(KVPairFin* kv);

    int Delete(const std::string& key);
    int Get(const std::string& key, std::string* val);
#ifdef NEED_SCAN
    int Scan(const std::string& beginKey, int n, std::vector<std::string>& output);
    // int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);
#endif
    // int Update(const std::string& key, const std::string& val);
    
    // interfaces for background use only
    int leafSplitCmp;
    int leafCmpCnt;
    int leafSearchCnt;
    int leafCnt;
    int leafSplitCnt;
    int innerUpdateCnt;
    int depth;
    int dupKeyCnt;
protected:
    KVLeafNodeFin* leafSearch(const char* key);
    uint8_t PearsonHash(const char* data, const size_t size);
    
    bool leafFillSlotForKey(KVLeafNodeFin* leafnode, const uint8_t hash, KVPairFin* kv);
    void leafSplitFull(KVLeafNodeFin* leafnode, KVPairFin* kv);
    void innerUpdateAfterSplit(KVNodeFin* node, std::unique_ptr<KVNodeFin> new_node, std::string* split_key);

    
private:
    std::unique_ptr<KVNodeFin> root; // root node of the B+tree
    std::shared_ptr<KVLeafFin> head; // list head of leafnodes.
};
 
}

#endif /* BplusTree_h */
