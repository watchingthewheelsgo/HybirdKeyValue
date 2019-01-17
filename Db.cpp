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
#include "SkipList.h"
#include "Thread.h"
#include "Configure.h"
#include "kvObject.h"
#include "HashTable.h"
#include "PmemKV.h"
#include "HiKV.h"

#include "latency.hpp"
#include "hyKV_def.h"

namespace hybridKV {
int hyDB::Open(hyDB** db, std::string dbname, int size) {
    *db = nullptr;
    if (dbname == "PmemKV") {
#ifndef HiKV_TEST
        PmemKV* newDB = new PmemKV();
        *db = newDB;
#endif
    } else if (dbname == "HiKV") {
#ifdef HiKV_TEST
        HiKV* newDB = new HiKV();
        //HiKV::BGWork(reinterpret_cast<void*>(newDB));
        *db = newDB;
#endif
    } else if (dbname == "hyKV") {
        DBImpl* newDB = new DBImpl(size);
        //DBImpl::BGWork(reinterpret_cast<void*>(newDB));
        *db = newDB;
    } else {
        LOG("Not Supported DB Name: " << dbname);
        return -1;
    }
    return 0;
}
DBImpl:: DBImpl(int size):
    cfg(new Config(size)),
    sl_size(cfg->sl_size),
    sl_grp(new slPointer [cfg->sl_size]),
    ht_(new HashTable(cfg->ht_size, cfg->ht_seed, cfg->ht_limits, cfg->hasher, true)),
    thrds(new ThreadPool(cfg->sl_size))
    {
        cfg->lastDb = ht_;
        cfg->sl_grp = sl_grp;
        cfg->thrds = thrds;
        LOG("Creating Skiplists...");
        for (int i=0; i<cfg->sl_size; ++i) {
            sl_grp[i] = new SkipList(i);
        }
        LOG("there are " << cfg->sl_size <<" skiplists created.");
}
//DBImpl:: DBImpl(){ }
// need to be implement
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
int DBImpl::Put(const std::string& key, const std::string& val) {
    kvObj* newKey = new kvObj(key, true);
    kvObj* value = new kvObj(val, true);
#ifdef PM_WRITE_LATENCY_TEST
    newKey->latency(true);
    value->latency(true);
#endif
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(newKey, value, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
  /*  
    int idx = cfg->slIndex(newKey->size());

    SkipList::Node* slNode = sl_grp[idx]->newNode((void*)newKey, (void*)value, true);
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)slNode, sizeof(slNode));
#endif
    slCmdNode* node = new slCmdNode;

    node->priv = entryPointer->slNodeAddr = (void*)slNode;
    node->key = nullptr;
    node->val = nullptr;
    node->type = kInsertType;
    sl_grp[idx]->cmd_push((void*)node);
*/
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
    
    // int idx = cfg->slIndex(rpKey->size());
    
    // slCmdNode* node = new slCmdNode;

    // node->priv = (void*)(entryPointer->slNodeAddr);
    // node->key = (void*)rpKey;
    // node->val = (void*)newVal;
    // node->type = kUpdateType;
    
    // sl_grp[idx]->cmd_push((void*)node);
    return 0;
}

int DBImpl::Delete(const std::string& key) {
    
    kvObj* delKey = new kvObj(key, false);
    if (ht_->Delete(key) == -1)
        return -1;
    
//     int idx = cfg->slIndex(delKey->size());
    
//     slCmdNode* node = new slCmdNode;
// #ifdef PM_WRITE_LATENCY_TEST
//     pflush((uint64_t*)node, sizeof(*node));
// #endif
//     node->key = (void*)delKey;
//     node->val = nullptr;
//     node->type = kDeleteType;
//     node->priv = nullptr;
    
//     sl_grp[idx]->cmd_push((void*)node);
//     return 0;
}
/*
int DBImpl::Scan(const std::string& beginKey, uint64_t n, std::vector<std::string>& output) {
    kvObj bgKey(beginKey, false);
    std::vector<scanRes*> result(sl_size);
    //std::vector<std::string*> part(sl_size, &std::string());
    for (int idx=0; idx<sl_size; ++idx) {
        slCmdNode* node = new slCmdNode;
        node->priv = (void*)n;
        node->key = (void*)(&bgKey);
        result[idx] = new scanRes();
        node->val = (void*)(&result[idx]);
        node->type = kScanNumType;
        
        sl_grp[idx]->cmd_push((void*)node);
    }
    for (int idx=0; idx<sl_size; ++idx) {
        while (reinterpret_cast<uint64_t>(result[idx]->done.Acquire_Load()) == 0);
    }
    
    mergeScan(result, output);
    
    return 0;
}
*/
int DBImpl::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) {
    kvObj bgKey(beginKey, false);
    kvObj edKey(lastKey, false);
    std::vector<scanRes*> result(sl_size);
    
    for (int idx=0; idx<sl_size; ++idx) {
        slCmdNode* node = new slCmdNode;
        node->priv = (void*)(&edKey);
        node->key = (void*)(&bgKey);
        result[idx] = new scanRes();
        node->val = (void*)(result[idx]);
        node->type = kScanNorType;
        
        sl_grp[idx]->cmd_push((void*)node);
    }
    for (int idx=0; idx<sl_size; ++idx) {
        while (reinterpret_cast<uint64_t>(result[idx]->done.Acquire_Load()) == 0);
    }
    
    mergeScan(result, output);
    
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
    for (int i=0; i<sl_size; ++i) {
        thrds->CreateJobs(schedule, reinterpret_cast<void*>(sl_grp[i]), i);
    }
}
    
void* schedule(void* arg) {
    //        assert(idx < sl_size);
//     SkipList* curSL = reinterpret_cast<SkipList*>(arg);
    
// //    std::cout << curSL->idx() <<std::endl;
//     // assert(idx == curSL->idx());
//     // maybe add control with onSchedule();
//     while (true) {
//         while (!curSL->emptyQue()) {
//             slCmdNode* cmd = curSL->extractCmd();
//             int res = 0;
// //            enum cmdType{
// //                kDeleteType = 0x0,
// //                kUpdateType = 0x1,
// //                kInsertType = 0x2,
// //                kScanNorType = 0x3,
// //                kScanNumType = 0x4
// //            };

//             // cmdType type = cmd->type;
//             // auto node = cmd->slNodeAddr;
//             // auto key = cmd->key;
//             // auto type = cmd->type;
//             // kDeleteType = 0x0, kUpdateType = 0x1, kInsertType = 0x2
//             switch (cmd->type) {
//                 case kDeleteType:
//                     res = curSL->Delete(cmd->key);
//                     delete (kvObj*)(cmd->key);
//                     break;
//                 case kUpdateType:
//                     res = curSL->Update(cmd->key, cmd->val, (SkipList::Node*)cmd->priv);
//                     delete (kvObj*)(cmd->key);
//                     break;
//                 case kInsertType:
//                     res = curSL->Insert((SkipList::Node*)cmd->priv);
//                     break;
//                 case kScanNumType:
//                     assert(curSL->emptyQue());
//                     res = curSL->Scan(cmd->key, (uint64_t)(cmd->priv), *(reinterpret_cast<scanRes*>(cmd->val)));
//                     break;
//                 case kScanNorType:
//                     assert(curSL->emptyQue());
//                     res = curSL->Scan(cmd->key, cmd->priv, *(reinterpret_cast<scanRes*>(cmd->val)));
//                     break;
//                 default:
//                     LOG("unKnown cmd type");
//                     res = -1;
//                     break;
//             }
//             if (res != 0) {
//                 LOG("Failed Operation upon SL");
//                 LOG("Error type " << cmd->type);
//                 continue;
//             }
//         }
//     }
}
void DBImpl::waitBGWork() {
    for (int i=0; i<sl_size; ++i) {
        sl_grp[i]->waitForBGWork();
    }
}
void DBImpl::slGeo() {
    std::cout << "-------------- skiplist kv data -------------- " <<std::endl;
    for (int i=0; i<sl_size; ++i) {
        std::cout << "sl "<< i <<std::endl;
        sl_grp[i]->scanAll();
        std::cout << std::endl;
    }
}
void DBImpl::showCfg() {
    std::cout << "Index Array: " <<std::endl;
    for (int i=0; i<cfg->max_key; ++i) {
        std::cout << cfg->slIndex(i) <<std::endl;
    }
}
int DBImpl::Recover() {
    return 0;
}

int DBImpl::Close() {
    return 0;
}


}

