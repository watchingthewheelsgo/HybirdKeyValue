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

#ifndef BplusTree_h
#define BplusTree_h

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

#include "hyKV_def.h"
#include "latency.hpp"

namespace hybridKV {

static const int INNER_KEYS = 4;
static const int INNER_KEYS_MIDPOINT = (INNER_KEYS / 2);
static const int INNER_KEYS_UPPER = INNER_KEYS_MIDPOINT + 1;
static const int LEAF_KEYS = 48;
static const int LEAF_KEYS_MIDPOINT = LEAF_KEYS / 2;

// persistent

// class KVPair {
//     public:
//         uint8_t hash() { return hash_; }
//         const char* key() const { return key_; }
//         const uint32_t keysize() const { return ksize_; }
//         const uint32_t valsize() const { return vsize_; }
// //        const char* key() { return kv; }
//         const char* val() { return val_; }
//         bool valid() { return hash_ != 0; }
//         void clear();
//         void Set(const uint8_t hash, const char* key, const char* value);
        
//     private:
//         uint8_t hash_;
//         uint32_t ksize_;
//         uint32_t vsize_;
//         const char* key_;
//         const char* val_;
// }; 

class KVPair {
public:
    uint8_t hash() { return hash_; }
    const char* key() const { return kv; }
    const uint32_t keysize() const { return ksize_; }
    const uint32_t valsize() const { return vsize_; }
    bool valid() { return hash_ != 0; }
    const char* key() { return kv; }
    const char* val() { return kv+ksize_+1; }
    
    void clear();
    void Set(const uint8_t hash, const std::string& key, const std::string& value);
    
private:
    uint8_t hash_;
    uint32_t ksize_;
    uint32_t vsize_;
    char* kv;
};

// persistent
struct KVLeaf {
    KVLeaf() {
        for (int i=0; i<LEAF_KEYS; ++i) {
            slots[i] = std::make_unique<KVPair>();
#ifdef PM_WRITE_LATENCY_TEST
            pflush((uint64_t*)(slots[i].get()), sizeof(KVPair));
#endif
        }
    }
    std::unique_ptr<KVPair> slots[LEAF_KEYS];
    KVLeaf* next;
};

// volatile
struct KVInnerNode;
struct KVNode {
    bool isLeaf = false;
    KVInnerNode* parent;
    virtual ~KVNode() { }
};
// volatile
struct KVInnerNode : KVNode {
    uint8_t keyCount;
    std::string keys[INNER_KEYS + 1];
    std::unique_ptr<KVNode> children[INNER_KEYS + 2];
};
// volatile
struct KVLeafNode : KVNode {
    uint8_t hashes[LEAF_KEYS];
    std::string keys[LEAF_KEYS];
    std::shared_ptr<KVLeaf> leaf;
#ifdef NEED_SCAN
    KVLeafNode* next;
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

class BplusTree {
public:
    BplusTree():dupKeyCnt(0), leafSplitCmp(0), leafCmpCnt(0), leafSplitCnt(0), innerUpdateCnt(0), leafCnt(0), leafSearchCnt(0), depth(0) {};
    ~BplusTree(){};
// #ifdef HiKV_TEST
//     int Insert(const char* key, const char* val);

// #else
    //general interfaces provided by Bplus tree
    int Put(const std::string& key, const std::string& val);
// #endif
    int Delete(const std::string& key);
    int Get(const std::string& key, std::string* val);
    int Scan(const std::string& beginKey, int n, std::vector<std::string>& output);

// #ifdef NEED_SCAN
// #ifdef HiKV_TEST
    // int Scan(const std::string& beginKey, const std::string& lastKey, scanRes& output);
// #endif
    int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);
// #endif
    int Update(const std::string& key, const std::string& val);
    
    void showAll() {
        if (!head) {
            LOG("No KV pair Yet.");
            return ;
        }
        int leafnumber = 0;
        for (auto itor = head.get(); itor != nullptr; itor = itor->next) {
            int idx = 0;
            for (int i=0; i<LEAF_KEYS; ++i) {
                if (itor->slots[i]->valid()) {
                    LOG("key " << idx << " : " << itor->slots[i]->key());
                    ++idx;
                }
            }
            ++leafnumber;
        }
        LOG("There are "<< leafnumber << "leafs.");
    }
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
    KVLeafNode* leafSearch(const std::string& key);
    uint8_t PearsonHash(const char* data, const size_t size);
    
// #ifdef HiKV_TEST
//     bool leafFillSlotForKey(KVLeafNode* leafnode, const uint8_t hash, const char* key, const char* val);
//     void leafFillSpecificSlot(KVLeafNode* leafnode, const uint8_t hash, const char* key, const char* val, const int slot);
//     void leafSplitFull(KVLeafNode* leafnode, const uint8_t hash, const char* key, const char* val);
//     void leafFillEmptySlot(KVLeafNode* leafnode, const uint8_t hash, const char* key, const char* val);
// #else
    bool leafFillSlotForKey(KVLeafNode* leafnode, const uint8_t hash, const std::string& key, const std::string& val);
    void leafFillSpecificSlot(KVLeafNode* leafnode, const uint8_t hash, const std::string& key, const std::string& val, const int slot);
    void leafSplitFull(KVLeafNode* leafnode, const uint8_t hash, const std::string& key, const std::string& val);
    void leafFillEmptySlot(KVLeafNode* leafnode, const uint8_t hash, const std::string& key, const std::string& val);
// #endif
    void innerUpdateAfterSplit(KVNode* node, std::unique_ptr<KVNode> new_node, std::string* split_key);

    
private:
    std::unique_ptr<KVNode> root; // root node of the B+tree
    std::shared_ptr<KVLeaf> head; // list head of leafnodes.

};
 
}

#endif /* BplusTree_h */
