//
//  HashTable.cpp
//  devl
//
//  Created by 吴海源 on 2018/12/20.
//  Copyright © 2018 why. All rights reserved.
//

#include "HashTable.h"
namespace hybridKV {

//void Dict::debugPrint() {
//    for (int i=0; i<size_; ++i) {
//        if (table[i] == nullptr)
//            std::cout << "index " << i << ": " << (long int)table[i] << " has no element!" <<std::endl;
//        else {
//            auto itor = table[i];
//            while (itor != nullptr) {
//                const char *tt = (reinterpret_cast<kvObj*>(itor->val))->data();
//                if (tt == nullptr)
//                    std::cout << "wrong when add, no data" <<std::endl;
//                std::cout << "index "<< i << " has element(s): " << tt << " ";
//                itor = itor->next;
//            }
//            std::cout << std::endl;
//        }
//    }
//}

int Dict::Set(kvObj *key, kvObj *value, dictEntry **entryPtr) {
    int ret = 0;
    // if ((*entryPtr = Add(key, value, &ret)) == nullptr) {
    //     if (ret == -1)
    //         ++dupKeyCnt;
    //     return ret;
    // }
    if (LookUp(key) == nullptr) {
        if ((*entryPtr = Add(key, value, &ret)) == nullptr) {
            LOG("ERROR WHEN ADD KEY\r\n");
            return ret;
        }
    } else {
        // ++dupKeyCnt;
        if ((*entryPtr = OverWrite(key, value)) == nullptr) {
            LOG("ERROR WHEN OVER WRITE\r\n");
            return -1;
        } else {
            return 1;
        }
    }
    return 0;
}

int Dict::Get(const kvObj* key, std::string* value) {
    kvObj* vo = nullptr;
    if ((vo = LookUp(key)) == nullptr) {
        // value->assign(100, 'a');
        // LOG("No such key");
        return -1;
    }
    //        kvObj *val = static_cast<kvObj *>(vo->data());
    value->assign(vo->data(), vo->size());
    return 0;
}

//return kvobj val if key exists, otherwise return null
Dict::dictEntry* Dict::getEntry(const kvObj* key) {
//    LOG("Function: getEntry()");
    Dict::dictEntry* entry;
    uint32_t hash_key = hasher(key->data(), key->size());
    int idx = hash_key & szMask();
    entry = table[idx];
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
    pload((uint64_t*)(&table[idx]), sizeof(void*));
#endif
#endif
    int conflict = 0;
    while (entry != nullptr) {

        if (equal(*key, *(kvObj*)(entry->key)))
            return entry;
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
        pload((uint64_t*)(&entry->next), sizeof(void*));
#endif
#endif
        ++conflict;
        conflictCntRd += conflict;
        if (maxConflictRd < conflict)
            maxConflictRd = conflict; 
        entry = entry->next;
    }

    return nullptr;
}

kvObj* Dict::LookUp(const kvObj* key) {
//    LOG("Function: LookUP()");
    Dict::dictEntry *entry = getEntry(key);
    if (entry != nullptr) {
        return (kvObj*)(entry->val);
    }
    return nullptr;
}

int Dict::getIndex(const kvObj* key) {
//    LOG("Function: getIndex()");
    uint32_t hash_key = hasher(key->data(), key->size());
    uint32_t idx = hash_key & szMask();
    dictEntry* entry = table[idx];
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
    pload((uint64_t*)(&table[idx]), sizeof(void*));
#endif
#endif
    int conflict = 0;
    while (entry != nullptr) {
        ++conflict;
        if (equal(*key, *(kvObj*)(entry->key)))
            return -1;
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
        pload((uint64_t*)(&entry->next), sizeof(void*));
#endif
#endif
        entry = entry->next;
    }
    conflictCnt += conflict;
    if (maxConflict < conflict)
        maxConflict = conflict; 
    return idx;
}

Dict::dictEntry* Dict::Add(kvObj* key, kvObj* value, int* ret) {
//    LOG("Function: DictAdd()");
    if ((used() + 1) == size()) {
        *ret = -2;
        return nullptr;
    }
    uint32_t idx;
    if ((idx = getIndex(key)) == -1) {
        *ret = -1;
        return nullptr;
    }
    // std::cout << "idx = " << idx <<std::endl;
    Dict::dictEntry* newEntry = new Dict::dictEntry();
    //    std:cout << ""
    newEntry->key = key;
    newEntry->val = value;
    newEntry->next = table[idx];

    table[idx] = newEntry;
#ifdef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)(newEntry), sizeof(dictEntry));
    pflush((uint64_t*)(&table[idx]), sizeof(void*));
#endif
#endif
    incrNode();
    
    return newEntry;
}

Dict::dictEntry* Dict::OverWrite(const kvObj* key, kvObj* value) {
//    LOG("Function: OverWrite() ");
    dictEntry* entry = nullptr;
    // get the entry contains the key value
    entry = getEntry(key);
    // key not exists
    if (entry == nullptr)
        return nullptr;
    // mark the val
    kvObj *tmp = (kvObj*)(entry->val);
    // set new value
    entry->val = value;
#ifdef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)(&entry->val), sizeof(void*));
#endif
#endif
    // free old value
    if (tmp != nullptr)
        delete tmp;
    // tmp->~kvObj();
    // if (tmp)
    //     delete tmp;
    return entry;
}

int Dict::Delete(const kvObj* key, int& bt) {
//    LOG("Function: Delete() ");
    dictEntry *tarEntry, *preEntry = nullptr;
    uint32_t hash_key = hasher(key->data(), key->size());
    
    uint32_t idx = hash_key & szMask();
    
    tarEntry = table[idx];
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
    pload((uint64_t*)(&table[idx]), sizeof(void *));
#endif
#endif
    //        preNode = nullptr;
    while (tarEntry) {
        if (*key == *(kvObj*)(tarEntry->key)) {
            if (preEntry != nullptr) {
                preEntry->next = tarEntry->next;
            } else {
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
                pload((uint64_t*)(&tarEntry->next), sizeof(void *));
#endif
#endif
                
                table[idx] = tarEntry->next;
                
#ifdef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
                pflush((uint64_t*)(&table[idx]), sizeof(void*));
#endif
#endif
            }
            bt = tarEntry->idx;
            delete tarEntry;
            decrNode();

            return 0;
        }
        preEntry = tarEntry;
        tarEntry = tarEntry->next;
#ifdef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
        pload((uint64_t*)(&table[idx]), sizeof(void *));
#endif
#endif
    }
    LOG(" No key to delete");
    return -1;
}


int HashTable::Get(const kvObj* key, std::string* value) {
//    LOG("HashTable::Get()");
    // kvObj* k = new kvObj(key, false);
    int res = dict_->Get(key, value);
//    LOG("Value: " << *value);
//    LOG("");
    return res;
}

int HashTable::Delete(const kvObj* key, int& bt) {
//    LOG("HashTable::Delete()");
    // kvObj* k = new kvObj(key, false);
    int res = dict_->Delete(key, bt);
//    LOG("");
    return res;
    
}


int HashTable::Get(const std::string& key, std::string* value) {
//    LOG("HashTable::Get()");
    kvObj* k = new kvObj(key, false);
    int res = dict_->Get(k, value);
//    LOG("Value: " << *value);
//    LOG("");
    return res;
}

int HashTable::Set(kvObj* key, kvObj* value, Dict::dictEntry** entryPointer) {
//    LOG("HashTable::Set()");
    int ret = dict_->Set(key, value, entryPointer);
    if (ret == -2) {
        Rehash();
        ret = dict_->Set(key, value, entryPointer);
        if (ret == -2)
            LOG("No more key");
    }
//    LOG("");
    return ret;
}

int HashTable::Delete(const std::string& key) {
//    LOG("HashTable::Delete()");
    kvObj* k = new kvObj(key, false);
    int x;
    int res = dict_->Delete(k, x);
//    LOG("");
    return res;
    
}

void HashTable::Rehash() {
    LOG("Rehashing...");
    uint32_t newSize = std::min(dict_->size()  << 1 , limits_);
    Dict* newDict = new Dict(newSize, hasher);
    Dict::Iterator iter(dict_);
    iter.SeekToFirst();
    Dict::dictEntry* ret = 0;
    while (iter.Valid()) {
        if (newDict->Set((kvObj*)(iter.Key()->key), (kvObj*)(iter.Key()->val), &ret) != -1) {
            dict_->decrNode();
        }
        uint32_t index = iter.index();
        iter.Next();
        dict_->remove(index);
    }
    assert(dict_->used() == 0);
    delete dict_;
    dict_ = newDict;
}
    
}
