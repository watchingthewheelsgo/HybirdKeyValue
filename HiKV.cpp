//
//  HiKV.cpp
//  devl
//
//  Created by 吴海源 on 2018/12/12.
//  Copyright © 2018 why. All rights reserved.
//

// HiKV are hybrid index, which consists of B+ tree in memory and Hash Table in pmem
#include <deque>

#include "HiKV.h"



namespace hybridKV {
    
#ifdef HiKV_TEST
    
HiKV::HiKV():cfg(new Config(' ')), tree_(new BplusTree),
    ht_(new HashTable(cfg->ht_size, cfg->ht_seed, cfg->ht_limits, cfg->hasher, false)),
    thrds(new ThreadPool(1)) {
    
}

void HiKV::BGWork(void* db) {
    reinterpret_cast<HiKV*>(db)->BgInit();
}

void HiKV::BgInit() {
    thrds->CreateJobs(schedule2, reinterpret_cast<void*>(this), 0);
}
void* schedule2(void* arg) {
    auto db = reinterpret_cast<HiKV*>(arg);
    auto bgTree = db->tree();
    while(true) {
        while (!db->emptyQue()) {
            cmdInfo* cmd = db->extraQue();
            cmdType type = cmd->type;
            int res = 0;
            switch (type) {
                case kInsertType:
                case kUpdateType:
                    res = bgTree->Insert(cmd->key, cmd->value);
                    break;
                case kDeleteType:
                    res = bgTree->Delete(std::string(cmd->key));
                    break;
#ifdef NEED_SCAN
                case kScanNorType:
                    res = bgTree->Scan(std::string(cmd->key), std::string(cmd->value), *(scanRes*)cmd->reserved);
                    break;
#endif
                default:
                    break;
            }
            if (res != 0)
                LOG("error");
        }
    }
    
    return nullptr;
}
// wrapper of "hashtable get"
int HiKV::Get(const std::string& key, std::string* val) {
    kvObj srhKey(key, false);
    if (ht_->Get(key, val) == -1)
        return -1;
    return 0;
}
//
int HiKV::Put(const std::string& key, const std::string& val) {
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
    auto cmd = new cmdInfo();
    cmd->type = kInsertType;
    cmd->key = newKey->data();
    cmd->value = value->data();
    
    queue_push(cmd);
*/
    return 0;
    
}

int HiKV::Update(const std::string& key, const std::string& val) {
    kvObj* rpKey = new kvObj(key, false);
    kvObj* newVal = new kvObj(val, true);
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(rpKey, newVal, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
    auto cmd = new cmdInfo();
    cmd->type = kUpdateType;
    cmd->key = rpKey->data();
    cmd->value = newVal->data();
    
    queue_push(cmd);
    return 0;
}
    
int HiKV::Delete(const std::string& key) {
    auto cmd = new cmdInfo();
    cmd->type = kDeleteType;
    cmd->key = key.data();
    
    queue_push(cmd);
    return 0;
}
#ifdef NEED_SCAN
int HiKV::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) {
    
    auto cmd = new cmdInfo();
    
    scanRes res;
    
    cmd->type = kScanNorType;
    cmd->key = beginKey.data();
    cmd->value = lastKey.data();
    cmd->reserved = (void*)(&res);
    
    queue_push(cmd);
    
    while (reinterpret_cast<uint64_t>(res.done.Acquire_Load()) == 0);
    while (!res.elems.empty()) {
        output.push_back(std::string((char *)res.elems.front()));
        res.elems.pop_front();
    }
    return 0;
}
#endif

#endif
}
