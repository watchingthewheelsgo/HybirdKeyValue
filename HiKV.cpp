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
    
HiKV::HiKV():cfg(new Config()), tree_(new BplusTreeList(0)), bgSchedule(true),
    ht_(new HashTable(cfg->ht_size, cfg->ht_limits, cfg->hasher, cfg->in_memory))
    {
        
}

void HiKV::BGWork(void* db) {
    reinterpret_cast<HiKV*>(db)->BgInit();
}

void HiKV::BgInit() {
    th.push_back(std::thread(schedule2,  reinterpret_cast<void*>(this)));
    // thrds->CreateJobs(schedule2, reinterpret_cast<void*>(this), 0);
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
                    res = bgTree->Insert((KVPairFin*)cmd->ptr);
                    db->tmr_all.stop();
                    break;
                case kDeleteType:
                    res = bgTree->Delete((kvObj*)cmd->key);
                    db->tmr_all.stop();
                    break;
                case kScanNorType:
                    res = bgTree->Scan((kvObj*)cmd->key, (kvObj*)cmd->value, (scanRes*)cmd->ptr);
                    db->tmr_all.stop();
                    break;
                case kFlushType:
                    db->bgSchedule = false;
                    break;
                default:
                    break;
            }
            if (res != 0)
                LOG("error");
        }
    }
    
    return nullptr;
}
void HiKV::newRound() {
    tmr_all.setZero();
    tmr_ht.setZero();
    bgSchedule = true;
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
    tmr_all.start();
    tmr_ht.start();
    kvObj* newKey = new kvObj(key, true);
    kvObj* value = new kvObj(val, true);
    KVPairFin* kv = new KVPairFin(newKey, value);
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)newKey, newKey->size());
    pflush((uint64_t*)value, value->size());
    pflush((uint64_t*)kv, sizeof(KVPairFin));
#endif
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(newKey, value, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
    auto cmd = new cmdInfo();
    cmd->type = kInsertType;
    cmd->key = nullptr;
    cmd->value = nullptr;
    cmd->ptr = (void*)kv;
    
    queue_push(cmd);
    tmr_ht.stop();
    return 0;
    
}

int HiKV::Update(const std::string& key, const std::string& val) {
    kvObj* rpKey = new kvObj(key, false);
    kvObj* newVal = new kvObj(val, true);
    KVPairFin* kv = new KVPairFin(rpKey, newVal);
#ifdef PM_WRITE_LATENCY_TEST
    // pflush((uint64_t*)newKey, newKey->size());
    pflush((uint64_t*)newVal, newVal->size());
    pflush((uint64_t*)kv, sizeof(KVPairFin));
#endif
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(rpKey, newVal, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
    auto cmd = new cmdInfo();
    cmd->type = kUpdateType;
    cmd->key = nullptr;
    cmd->value = nullptr;
    cmd->ptr = (void*)kv;

    queue_push(cmd);
    return 0;
}
    
int HiKV::Delete(const std::string& key) {

    kvObj* delKey = new kvObj(key, false);
    Dict::dictEntry* entryPointer = nullptr;
    int idx;
    if (ht_->Delete(delKey, idx) == -1)
        return -1;
    
    auto cmd = new cmdInfo();
    cmd->type = kDeleteType;
    cmd->key = delKey;
    cmd->value = nullptr;
    cmd->ptr = nullptr;

    queue_push(cmd);
    return 0;
}
int HiKV::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) {
    
    kvObj* bgKey = new kvObj(beginKey, false);
    kvObj* edKey = new kvObj(lastKey, false);

    auto cmd = new cmdInfo();
    
    scanRes res;
    
    cmd->type = kScanNorType;
    cmd->key = bgKey;
    cmd->value = edKey;
    cmd->ptr = (void*)(&res);
    
    queue_push(cmd);
    
    while (reinterpret_cast<uint64_t>(res.done.Acquire_Load()) == 0);
    while (!res.elems.empty()) {
        output.push_back(std::string(res.elems.front()));
        res.elems.pop_front();
    }
    return 0;
}

}
