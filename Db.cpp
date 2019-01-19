//
//  Db.cpp
//  devl
//
//  Created by 吴海源 on 2018/11/1.
//  Copyright © 2018 why. All rights reserved.
//
#include <iostream>
#include <algorithm>
#include "Db.h"
// #include "SkipList.h"
#include "BplusTreeList.h"
#include "Thread.h"
#include "Configure.h"
#include "kvObject.h"
#include "HashTable.h"
#include "PmemKV.h"
#include "HiKV.h"

#include "latency.hpp"
#include "hyKV_def.h"

namespace hybridKV {
int hyDB::Open(hyDB** db, const std::string dbname, int size) {
    *db = nullptr;
    if (dbname == "PmemKV") {
        PmemKV* newDB = new PmemKV();
        *db = newDB;
    } else if (dbname == "HiKV") {
        HiKV* newDB = new HiKV();
        // HiKV::BGWork(reinterpret_cast<void*>(newDB));
        *db = newDB;
    } else if (dbname == "myKV") {
        DBImpl* newDB = new DBImpl(size);
        DBImpl::BGWork(reinterpret_cast<void*>(newDB));
        *db = newDB;
    } else {
        LOG("Not Supported DB Name: " << dbname);
        return -1;
    }
    return 0;
}
DBImpl:: DBImpl(int size):
    cfg(new Config(size)),
    bt_size(cfg->split_size),
    bt_grp(new btPointer [cfg->split_size]),
    ht_(new HashTable(cfg->ht_size, cfg->ht_limits, cfg->hasher, cfg->in_memory)),
    thrds(new ThreadPool(cfg->split_size))
    {
        cfg->lastDb = ht_;
        cfg->bt_grp = bt_grp;
        cfg->thrds = thrds;
        LOG("Creating BplusTrees...");
        for (int i=0; i<cfg->split_size; ++i) {
            bt_grp[i] = new BplusTreeList(i);
        }
        LOG("there are " << cfg->split_size <<" Bplus Trees created.");
}

// need to be implement
// sorry for the memory leak till now :(.
DBImpl::~DBImpl() {
    
}
// wrapper of "hashtable get"
int DBImpl::Get(const std::string& key, std::string* val) {
    kvObj srhKey(key, false);
    if (ht_->Get(key, val) == -1)
        return -1;
    return 0;
}
//
int DBImpl::getApproIndex() {
    if (bt_size == 1)
        return 0;
    int x=0, min=bt_grp[0]->busyCnt(), k;
    for (int k=1; k<bt_size; ++k) {
        int tmp = bt_grp[k]->busyCnt();
        if (tmp < min) {
            x = k;
            min = tmp;
        }
    }
    return x;
}
int DBImpl::Put(const std::string& key, const std::string& val) {
    kvObj* newKey = new kvObj(key, true);
    kvObj* value = new kvObj(val, true);
#ifdef PM_WRITE_LATENCY_TEST
    newKey->latency(true);
    value->latency(true);
#endif
    Dict::dictEntry* entryPointer = nullptr;
    int x = ht_->Set(newKey, value, &entryPointer);
    if (x == -1 || entryPointer == nullptr)
        return -1;
    if (x == 0) {
        int idx = getApproIndex();
        auto pair = new KVPairFin(newKey, value);

    #ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)pair, sizeof(KVPairFin));
    #endif
        btCmdNode* node = new btCmdNode;

        entryPointer->idx = idx;
        node->ptr = entryPointer->btNodeAddr = (void*)pair;
        node->key = nullptr;
        node->val = nullptr;
        node->type = kInsertType;
        bt_grp[idx]->cmd_push((void*)node);
    } else {
        // reserved to change to write-batch version
        int idx = entryPointer->idx;
        btCmdNode* node = new btCmdNode;

        node->ptr = (void*)(entryPointer->btNodeAddr);
        node->key = (void*)nullptr;
        node->val = (void*)value;
        node->type = kUpdateType;

        bt_grp[idx]->cmd_push((void*)node);
        delete newKey;
    }
    return 0;
    
}

int DBImpl::Update(const std::string& key, const std::string& val) {
    kvObj* rpKey = new kvObj(key, false);
    kvObj* newVal = new kvObj(val, true);
#ifdef PM_WRITE_LATENCY_TEST
    newVal->latency(true);
#endif
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(rpKey, newVal, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
    
    int idx = entryPointer->idx;
    
    btCmdNode* node = new btCmdNode;

    node->ptr = (void*)(entryPointer->btNodeAddr);
    node->key = nullptr;
    node->val = (void*)newVal;
    node->type = kUpdateType;
    
    bt_grp[idx]->cmd_push((void*)node);
    return 0;
}

int DBImpl::Delete(const std::string& key) {
    
    kvObj* delKey = new kvObj(key, false);
    Dict::dictEntry* entryPointer = nullptr;
    int idx;
    if (ht_->Delete(delKey, idx) == -1)
        return -1;
    
    btCmdNode* node = new btCmdNode;
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)node, sizeof(*node));
#endif
    node->key = (void*)delKey;
    node->val = nullptr;
    node->type = kDeleteType;
    node->ptr = nullptr;
    
    bt_grp[idx]->cmd_push((void*)node);
    return 0;
}

int DBImpl::Scan(const std::string& beginKey, int n, std::vector<std::string>& output) {
    // kvObj bgKey(beginKey, false);
    // std::vector<scanRes*> result(sl_size);
    // //std::vector<std::string*> part(sl_size, &std::string());
    // for (int idx=0; idx<sl_size; ++idx) {
    //     slCmdNode* node = new slCmdNode;
    //     node->priv = (void*)n;
    //     node->key = (void*)(&bgKey);
    //     result[idx] = new scanRes();
    //     node->val = (void*)(&result[idx]);
    //     node->type = kScanNumType;
        
    //     sl_grp[idx]->cmd_push((void*)node);
    // }
    // for (int idx=0; idx<sl_size; ++idx) {
    //     while (reinterpret_cast<uint64_t>(result[idx]->done.Acquire_Load()) == 0);
    // }
    
    // mergeScan(result, output);
    
    // return 0;
}

int DBImpl::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) {
    kvObj bgKey(beginKey, false);
    kvObj edKey(lastKey, false);
    std::vector<scanRes*> result(bt_size);
    
    for (int idx=0; idx<bt_size; ++idx) {
        btCmdNode* node = new btCmdNode;
        node->val = (void*)(&edKey);
        node->key = (void*)(&bgKey);
        result[idx] = new scanRes();
        node->ptr = (void*)(result[idx]);
        node->type = kScanNorType;
        
        bt_grp[idx]->cmd_push((void*)node);
    }
    for (int idx=0; idx<bt_size; ++idx) {
        while (reinterpret_cast<uint64_t>(result[idx]->done.Acquire_Load()) == 0);
    }
    
    // mergeScan(result, output);
    
    return 0;
}


void DBImpl::mergeScan(std::vector<scanRes*>& result, std::vector<std::string>& output) {
    LOG("Merging...");
    assert(!result.empty());
    auto cmp = [](scanRes* a, scanRes* b) {
        if (a == nullptr || a->elems.empty())
            return false;
        else if (b==nullptr || b->elems.empty())
            return true;
        return true;
//        return *(a->elems.front()) > *(b->elems.front());
    };
    make_heap(result.begin(), result.end(), cmp);
    while (!result.empty()) {
        pop_heap(result.begin(), result.end(), cmp);
        scanRes* top = result.back();
        if (top != nullptr && !(top->elems.empty())) {
            output.push_back(std::string(((kvObj*)(top->elems.front()))->data()));
            top->elems.pop_front();
            if (top->elems.empty()) {
                result.pop_back();
            }
        } else {
            result.pop_back();
        }
        push_heap(result.begin(), result.end(), cmp);
    }
    
    assert(result.empty());
}

// BackGround thread work Init for Skiplist
void DBImpl::BGWork(void* db) {
    reinterpret_cast<DBImpl*>(db)->BgInit();
}
    
void DBImpl::BgInit() {
    for (int i=0; i<bt_size; ++i) {
        thrds->CreateJobs(schedule, reinterpret_cast<void*>(bt_grp[i]), i);
    }
}
    
void* schedule(void* arg) {
    BplusTreeList* curBT = reinterpret_cast<BplusTreeList*>(arg);
    
    // maybe add control with onSchedule();
    while (true) {
        while (!curBT->emptyQue()) {
            btCmdNode* cmd = curBT->extractCmd();
            int res = 0;
//            enum cmdType{
//                kDeleteType = 0x0,
//                kUpdateType = 0x1,
//                kInsertType = 0x2,
//                kScanNorType = 0x3,
//                kScanNumType = 0x4
//            };
            // struct btCmdNode {
            //     cmdType type;
            //     void* key;
            //     void* val;
            //     void* ptr;
            // };

            // cmdType type = cmd->type;
            // auto node = cmd->slNodeAddr;
            // auto key = cmd->key;
            // auto type = cmd->type;
            // kDeleteType = 0x0, kUpdateType = 0x1, kInsertType = 0x2
            switch (cmd->type) {
                case kDeleteType:
                    res = curBT->Delete((kvObj*)cmd->key);
                    delete (kvObj*)(cmd->key);
                    break;
                case kUpdateType:
                    res = curBT->Update((KVPairFin*)cmd->ptr, (kvObj*)cmd->val);
                    delete (kvObj*)(cmd->key);
                    break;
                case kInsertType:
                    res = curBT->Insert((KVPairFin*)cmd->ptr);
                    break;
                // case kScanNumType:
                //     assert(curSL->emptyQue());
                //     res = curSL->Scan(cmd->key, (uint64_t)(cmd->priv), *(reinterpret_cast<scanRes*>(cmd->val)));
                //     break;
                case kScanNorType:
                    assert(curBT->emptyQue());
                    res = curBT->Scan((kvObj*)cmd->key, (kvObj*)cmd->val, reinterpret_cast<scanRes*>(cmd->ptr));
                    break;
                default:
                    LOG("unKnown cmd type");
                    res = -1;
                    break;
            }
            if (res != 0) {
                LOG("Failed Operation upon SL");
                LOG("Error type " << cmd->type);
                continue;
            }
        }
    }
}
// void DBImpl::waitBGWork() {
//     for (int i=0; i<sl_size; ++i) {
//         sl_grp[i]->waitForBGWork();
//     }
// }
// void DBImpl::slGeo() {
//     std::cout << "-------------- skiplist kv data -------------- " <<std::endl;
//     for (int i=0; i<sl_size; ++i) {
//         std::cout << "sl "<< i <<std::endl;
//         sl_grp[i]->scanAll();
//         std::cout << std::endl;
//     }
// }
// void DBImpl::showCfg() {
//     std::cout << "Index Array: " <<std::endl;
//     for (int i=0; i<cfg->max_key; ++i) {
//         std::cout << cfg->slIndex(i) <<std::endl;
//     }
// }
int DBImpl::Recover() {
    return 0;
}

int DBImpl::Close() {
    return 0;
}


}

