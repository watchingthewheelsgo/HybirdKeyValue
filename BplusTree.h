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
#include <deque>
#include <unordered_map>
#include <atomic>

#include <boost/lockfree/spsc_queue.hpp>
#include <boost/atomic.hpp>

#include "atomic_pointer.h"
#include "DBInterface.h"
#include "Tool.h"
#include "Configure.h"

#include "hyKV_def.h"
#include "latency.hpp"
#include "kvObject.h"
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

static const int INNER_KEYS = 8;
static const int INNER_KEYS_MIDPOINT = (INNER_KEYS / 2);
static const int INNER_KEYS_UPPER = INNER_KEYS_MIDPOINT + 1;
static const int LEAF_KEYS = 24;
static const int LEAF_KEYS_MIDPOINT = LEAF_KEYS / 2;

// persistent
struct cmdInfo {
    cmdType type;
    void* key;
    void* value;
    void* ptr;
};

class KVPairSplit {
    public:
        uint8_t hash() { return hash_; }
        const char* key() const { return key_->data(); }
        const uint32_t keysize() const { return key_->size(); }
        const uint32_t valsize() const { return val_->size(); }
        const char* val() { return val_->data(); }
        bool valid() { return hash_ != 0; }
        void clear();
        void Set(const uint8_t hash, kvObj* key, kvObj* val);
        
    private:
        uint8_t hash_;
        kvObj* key_;
        kvObj* val_;
        // const char* key_;
        // const char* val_;
}; 
// persistent
struct KVLeafSplit {
    KVLeafSplit() {
        for (int i=0; i<LEAF_KEYS; ++i) {
            slots[i] = std::make_unique<KVPairSplit>();
        }
    }
    std::unique_ptr<KVPairSplit> slots[LEAF_KEYS];
    KVLeafSplit* next;
};

// volatile
struct KVInnerNodeSplit;
struct KVNodeSplit {
    bool isLeaf = false;
    KVInnerNodeSplit* parent;
    virtual ~KVNodeSplit() { }
};
// volatile
struct KVInnerNodeSplit : KVNodeSplit {
    // KVInnerNode():keyCount(0), keys(INNER_KEYS+1) {}
    uint8_t keyCount;
    std::string keys[INNER_KEYS + 1];
    // std::vector<std::string> keys;
    std::unique_ptr<KVNodeSplit> children[INNER_KEYS + 2];
};
// volatile
struct KVLeafNodeSplit : KVNodeSplit {
    uint8_t hashes[LEAF_KEYS];
    std::string keys[LEAF_KEYS];
    std::shared_ptr<KVLeafSplit> leaf;
    KVLeafNodeSplit* next;
};

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
    // KVInnerNode():keyCount(0), keys(INNER_KEYS+1) {}
    uint8_t keyCount;
    std::string keys[INNER_KEYS + 1];
    // std::vector<std::string> keys;
    std::unique_ptr<KVNode> children[INNER_KEYS + 2];
};
// volatile
struct KVLeafNode : KVNode {
    uint8_t hashes[LEAF_KEYS];
    std::string keys[LEAF_KEYS];
    std::shared_ptr<KVLeaf> leaf;
    KVLeafNode* next;
};

class BplusTreeSplit {
public:
    // typedef boost::lockfree::spesc_queue<cmdInfo*, boost::lockfree::capacity<1 << 21>>::iterator QITOR;
    typedef std::deque<cmdInfo*>* QPTR;
    typedef std::deque<cmdInfo*>::reference QREF;
    BplusTreeSplit(int i): writeCnt(0), idx(i), qSize(0), mu_(new std::deque<cmdInfo*>()), mp_(new std::unordered_map<kvObj*, QREF>()), immu_(nullptr), existScan(false), flush(false)
    { };
    ~BplusTreeSplit(){ };

    //general interfaces provided by BplustreeSplit
    int Insert(kvObj* key, kvObj* val);
    int Delete(const std::string& key);
    int Delete(kvObj* key);
    int Get(const std::string& key, std::string* val);
    int Scan(const std::string& beginKey, int n, std::vector<std::string>& output);
    int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);
    int Update(kvObj* key, kvObj* val);

// #ifndef HiKV_TEST
    // Interfaces for Que Operations
//--------------------------------------------------
//    use lock free spsc_queue
//---------------------------------------------------
    void lockfreePush(cmdInfo* cmd) {
        // auto res = 
        que2.push(cmd);
        ++qSize;
    }

    cmdInfo* lockfreePop() {
        cmdInfo* res;
        que2.pop(res);
        --qSize;
        return res;
    }

//--------------------------------------------------
//  use std::deque<> and Mutex
//--------------------------------------------------
    void cmdPush(cmdInfo* cmd) {
        mtx_.lock();
        auto res = mp.find((kvObj*)cmd->key);
        if (res != mp.end()) {
            res->second->value = cmd->value;
        } else {
            que.push_back(cmd);
            ++qSize;
            mp.insert({(kvObj*)cmd->key, que.back()});
        }
        mtx_.unlock();
    }

    cmdInfo* cmdPop() {
        mtx_.lock();
        cmdInfo* cmd = que.front();
        mp.erase((kvObj*)cmd->key);
        que.pop_front();
        --qSize;
        mtx_.unlock();
        return cmd;
    }
//-------------------------------------------------------
//  use two std::deque without mutex
//------------------------------------------------------

    void mutPush(cmdInfo* cmd) {
        assert(mu_ != nullptr);
        if (flush.load() == false && (cmd->type == kFlushType || cmd->type == kScanNorType || cmd->type == kScanNumType))
            flush.store(true);
        // mtx_.lock();
        if (cmd->type == kFlushType) {
            mu_->push_back(cmd);
            ++qSize;
        } else {
            auto res = mp_->find((kvObj*)cmd->key);
            if (res != mp_->end()) {
                res->second->value = cmd->value;
            } else {
                mu_->push_back(cmd);
                ++qSize;
                mp_->insert({(kvObj*)cmd->key, mu_->back()});
            }
        }
        // mtx_.unlock();
        // mu_->push_back(cmd);
        // ++qSize;   
        if (qSize.load() > 100 || flush.load() == true) {    
        
            if (immu_.load() == nullptr) {
                // mtx_.lock();
                immu_.store(mu_);
                mu_ = new std::deque<cmdInfo*>();
                delete mp_;
                mp_ = new std::unordered_map<kvObj*, QREF>();
                qSize.store(0);
                flush.store(false);
                // mtx_.unlock();
            }
        }
    }
    bool dumpReady() {
        return immu_.load() != nullptr;
    }
    QPTR getList() {
        return immu_.load();
    }
    void resetList() {
        immu_.store(nullptr);
    }
    // void immutPop() {

    // }
    bool finished() {
        if (immu_.load() == nullptr && qSize.load() > 0) {
            auto cmd = new cmdInfo();
            cmd->type = kFlushType;
            mutPush(cmd);
        }
        return (qSize.load() == 0) && (immu_.load() == nullptr);
    }
    bool flushFlag() {
        return flush.load();
    }
    void setFlush() {
        flush.store(true);
    }
    int queSize() {
        return qSize.load();
    }

    void clearStats() {
        tmr.setZero();
        writeCnt = 0;
    }

// #endif
    // to measure BG time consumtion and write count.
    TimerRDT tmr;
    uint64_t writeCnt;
protected:
    int idx;
    KVLeafNodeSplit* leafSearch(const char* key);
    uint8_t PearsonHash(const char* data, const size_t size);

    void innerUpdateAfterSplit(KVNodeSplit* node, std::unique_ptr<KVNodeSplit> new_node, std::string* split_key);    
    bool leafFillSlotForKey(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val);
    void leafFillSpecificSlot(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val, const int slot);
    void leafSplitFull(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val);
    void leafFillEmptySlot(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val);
 
    
private:

// #ifndef HiKV_TEST
    boost::lockfree::spsc_queue<cmdInfo*, boost::lockfree::capacity<1 << 21>> que2; // BG cmd que
    std::deque<cmdInfo*> que;
    std::unordered_map<kvObj*, QREF> mp;
    std::unordered_map<kvObj*, QREF>* mp_;  // Record existing key in the que
    Mutex mtx_;

    QPTR mu_;
    std::atomic<QPTR> immu_;
    bool existScan;
    std::atomic<bool> flush;

// #endif
    std::atomic<int> qSize;
    std::unique_ptr<KVNodeSplit> root; // root node of the B+tree
    std::shared_ptr<KVLeafSplit> head; // list head of leafs.

};


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
    
    // interfaces for background use only
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
    Mutex mtx_;
    std::unique_ptr<KVNode> root; // root node of the B+tree
    std::shared_ptr<KVLeaf> head; // list head of leafnodes.

};
 
}

#endif /* BplusTree_h */
