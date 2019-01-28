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
    
// int HiKV::cnt = 0;
HiKV::HiKV():cfg(new Config()), tree_(new BplusTreeSplit(0)), bgSchedule(true), thrds(new ThreadPool(cfg->split_size)), qSize(0),
    ht_(new HashTable(cfg->ht_size, cfg->ht_limits, cfg->hasher, cfg->in_memory))
    {
        
}

void HiKV::BGWork(void* db) {
    reinterpret_cast<HiKV*>(db)->BgInit();
}

void HiKV::BgInit() {
    // th.push_back(std::thread(schedule2,  reinterpret_cast<void*>(this)));
    thrds->CreateJobs(schedule2, reinterpret_cast<void*>(this), 0);
}
void* schedule2(void* arg) {
    auto db = reinterpret_cast<HiKV*>(arg);
    auto bgTree = db->tree();
    while(true) {
        // bool loop = db->emptyQue();
        while (db->queSize() > 0) {
            // LOG(db->tree()->queSize());
            // cmdInfo* cmd = db->extraQue();
            cmdInfo* cmd = db->freePop();
            cmdType type = cmd->type;
            int res = 0;
            switch (type) {
                // case kRoundStart:
                //     bgTree->tmr.start();
                //     break;
                case kInsertType:
                    bgTree->tmr.start();
                    res = bgTree->Insert((kvObj*)cmd->key, (kvObj*)cmd->value);
                    bgTree->tmr.stop();
                    break;
                case kUpdateType:
                    bgTree->tmr.start();
                    res = bgTree->Update((kvObj*)cmd->key, (kvObj*)cmd->value);
                    bgTree->tmr.stop();
                    break;
                case kDeleteType:
                    // LOG(3);
                    bgTree->tmr.start();
                    res = bgTree->Delete((kvObj*)cmd->key);
                    delete (kvObj*)cmd->key;
                    bgTree->tmr.stop();
                    break;
                case kScanNorType:
                    bgTree->tmr.start();
                    res = bgTree->Scan((kvObj*)cmd->key, (kvObj*)cmd->value, ((std::vector<std::string>*)cmd->ptr));
                    bgTree->tmr.stop();
                    break;
                // case kRoundStop:
                //     // LOG(5);
                //     bgTree->tmr.stop();
                //     break;
                default:
                    break;
            }
            if (res != 0)
                LOG("error");
        }
        // LOG(1);
    }
    
    return nullptr;
}
// void HiKV::newRound() {
//     // tmr_all.setZero();
//     tmr_ht.setZero();
//     bgSchedule = true;
// }

// wrapper of "hashtable get"
int HiKV::Get(const std::string& key, std::string* val) {
    kvObj srhKey(key, false);
    if (ht_->Get(key, val) == -1)
        return -1;
    return 0;
}
//
int HiKV::Put(const std::string& key, const std::string& val) {
    // tmr_all.start();
    // tmr_ht.start();
    // ++HiKV::cnt;
    kvObj* newKey = new kvObj(key, true);
    kvObj* value = new kvObj(val, true);
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)newKey->data(), newKey->size());
    pflush((uint64_t*)value->data(), value->size());
#endif
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(newKey, value, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
    auto cmd = new cmdInfo();
    cmd->type = kInsertType;
    cmd->key = (void*)newKey;
    cmd->value = (void*)value;
    cmd->ptr = nullptr;
    
    // queue_push(cmd);
    freePush(cmd);
    // mtx_.lock();
    // tree_->cmdPush(cmd);
    // tree_->lockfreePush(cmd);
    // ++qSize;
    // mtx_.unlock();
    // LOG("p" << HiKV::cnt);
    // LOG("queSize = " << this->queSize());
    // tmr_ht.stop();
    return 0;
    
}

int HiKV::Update(const std::string& key, const std::string& val) {
    kvObj* rpKey = new kvObj(key, false);
    kvObj* newVal = new kvObj(val, true);
    // KVPairFin* kv = new KVPairFin(rpKey, newVal);
#ifdef PM_WRITE_LATENCY_TEST
    // pflush((uint64_t*)newKey, newKey->size());
    pflush((uint64_t*)newVal->data(), newVal->size());
    // pflush((uint64_t*)kv, sizeof(KVPairFin));
#endif
    Dict::dictEntry* entryPointer = nullptr;
    if (ht_->Set(rpKey, newVal, &entryPointer) == -1 || entryPointer == nullptr)
        return -1;
    // LOG("appending");
    auto cmd = new cmdInfo();
    cmd->type = kUpdateType;
    cmd->key = (void*)rpKey;
    cmd->value = (void*)newVal;
    cmd->ptr = nullptr;

    // queue_push(cmd);
    freePush(cmd);
    // mtx_.lock();
    // tree_->cmdPush(cmd);
    // ++qSize;
    // mtx_.unlock();
    // LOG(this->queSize());
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
    cmd->key = (void*)delKey;
    cmd->value = nullptr;
    cmd->ptr = nullptr;
    freePush(cmd);
    // queue_push(cmd);
    return 0;
}
int HiKV::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>* output) {
    
    kvObj* bgKey = new kvObj(beginKey, true);
    kvObj* edKey = new kvObj(lastKey, true);
    // std::vector<std::string> res;
    auto cmd = new cmdInfo();
    // scanRes res;
    cmd->type = kScanNorType;
    cmd->key = (void*)bgKey;
    cmd->value = (void*)edKey;
    cmd->ptr = (void*)(output);
    freePush(cmd);
    // queue_push(cmd);
    
    // while (reinterpret_cast<uint64_t>(res.done.Acquire_Load()) == 0);
    // while (!res.elems.empty()) {
    //     output.push_back(std::string(res.elems.front()));
    //     res.elems.pop_front();
    // }
    return 0;
}

}
