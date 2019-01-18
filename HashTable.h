#ifndef __HASH_TABLE_H
#define __HASH_TABLE_H

#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <assert.h>
#include "Configure.h"
#include "Thread.h"
#include "Tool.h"
#include "kvObject.h"

#include "hyKV_def.h"
#include "latency.hpp"

namespace hybridKV{

class kvObj;

class Dict {
public:
//    struct dictEntry;
    struct dictEntry {
        void* key;
        void* val;
        void* btNodeAddr;
        dictEntry* next;
        int idx;
    };
    class Iterator {
    public:
        explicit Iterator(Dict* dict);
        bool Valid() const;
        dictEntry* Key();
        void Next();
        void Prev();
        void Seek();
        void SeekToFirst();
        void SeelToLast();
        uint32_t index() {
            return curIdx;
        }
    private:
        uint32_t curIdx;
        Dict* dict_;
        dictEntry* entry_;
    };
    typedef dictEntry* entryPointer;
//    typedef uint32_t (*hashFunc)(const void*, size_t, uint32_t);
	// Dict():
	explicit Dict(uint32_t size, hashFunc hash) : size_(size),
		mask_(size - 1), 
		used_(0), 
		// seed_(seed), 
//        limits_(limits),
        conflictCnt(0),
        maxConflict(0),
		table(nullptr),
        hasher(hash) {
			table = new entryPointer [size_]; 
			for (int i=0; i<size_; ++i)
				table[i] = nullptr;
#ifdef PM_WRITE_LATENCY_TEST
            pflush((uint64_t*)(table), sizeof(void*) * size_);
#endif
	}
	~Dict() {
		assert(used_ == 0);
		if (table != nullptr)
			delete table;
	}
	uint32_t size() { return size_; }
	uint32_t used() { return used_; }
//    uint32_t limits() { return limits_; }
	uint32_t szMask() { return mask_; }
    
    int getIndex(const kvObj* key);
    dictEntry* getEntry(const kvObj* key);
    kvObj* LookUp(const kvObj* key);
    
    int Set(kvObj* key, kvObj* value, dictEntry** entryPtr);
    int Get(const kvObj* key, std::string* value);
    int Delete(const kvObj* key, int& bt);
    
    void remove(uint32_t idx) {
        assert(table[idx] != nullptr);
        dictEntry* victim = table[idx];
        table[idx] = table[idx]->next;
        delete victim;
    }
	void reset(int i) {
		assert(i < size_);
		table[i] = nullptr;
// #ifdef PM_WRITE_LATENCY_TEST
      // pflush((uint64_t*)(&table[i]), sizeof(void*));
// #endif
	}
    dictEntry* getHeadIndex(uint32_t index) {
        return table[index];
    }
	void decrNode() { 
		assert(used_ > 0);
		--used_;
// #ifdef PM_WRITE_LATENCY_TEST
//         pflush((uint64_t*)(&used_), sizeof(uint32_t));
// #endif
		assert(used_ >= 0);
	}
    void incrNode() { 
    	++used_;
// #ifdef PM_WRITE_LATENCY_TEST
//         pflush((uint64_t*)(&used_), sizeof(uint32_t));
// #endif
    }
    // Test Use. declared to test "Hash Conflict". 
    int maxConflict;
    int conflictCnt;
    int maxConflictRd;
    int conflictCntRd;
    int dupKeyCnt;
private:
	// private method to implement Dict::Set();
    dictEntry* Add(kvObj* key, kvObj* value, int* ret);
    dictEntry* OverWrite(const kvObj* key, kvObj* value);

    hashFunc hasher;
	uint32_t size_;
	uint32_t mask_;
	uint32_t used_;
	dictEntry** table;
};
inline Dict::Iterator::Iterator(Dict* dict) {
    dict_ = dict;
    entry_ = nullptr;
    curIdx = 0;
}

inline bool Dict::Iterator::Valid() const {
    return entry_ != nullptr;
}

inline Dict::dictEntry* Dict::Iterator::Key() {
    return entry_;
}

inline void Dict::Iterator::Next() {
    if (entry_->next != nullptr)
        entry_ = entry_->next;
    else {
        while (++curIdx < dict_->size()) {
            if (dict_->table[curIdx] != nullptr) {
                entry_ = dict_->table[curIdx];
                break;
            }
        }
    }
}
inline void Dict::Iterator::SeekToFirst() {
    while (curIdx < dict_->size()) {
        if (dict_->table[curIdx] != nullptr) {
            entry_ = dict_->table[curIdx];
            break;
        }
        ++curIdx;
    }
}

// 

// Wrapper of Dict. Provide Rehash(); to grow dict capacity.
// Mutex can be used to implement fine-grained lock on dictEntry;
// In current HT version, no concurrency data structure is considered.
class HashTable {
public:

    HashTable(uint32_t size, uint32_t limits, hashFunc hash, bool in_memory): mtx_(), limits_(limits), hasher(hash), dict_(new Dict(size, hash)), is_memory(in_memory) {

    }
    int Set(kvObj* key, kvObj* value, Dict::dictEntry** entrypointer);
    int Get(const std::string& key, std::string* value);
    int Delete(const std::string& key);
    
    int Get(const kvObj* key, std::string* value);
    int Delete(const kvObj* key, int& bt);
    
    // provided to printf conflict info of dict. 
    int conflictCnt() {
        return dict_->conflictCnt;
    }
    int maxConflict() {
        return dict_->maxConflict;
    }
    int conflictCntRd() {
        return dict_->conflictCntRd;
    }
    int maxConflictRd() {
        return dict_->maxConflictRd;
    }
    int dupKeyCnt() {
        return dict_->dupKeyCnt;
    }
    void setConflictRdZero() {
        dict_->maxConflictRd=0;
        dict_->conflictCntRd=0;
        dict_->dupKeyCnt = 0;
    }
private:
    void Rehash();

    bool is_memory;
    Mutex mtx_;
    uint32_t seed_;
    uint32_t limits_; // to limit table capacity
    hashFunc hasher;
    Dict* dict_;
};
    
    
}

#endif
