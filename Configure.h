//
//  Configure.h
//  devl
//
//  Created by 吴海源 on 2018/11/5.
//  Copyright © 2018 why. All rights reserved.
//

#ifndef Configure_h
#define Configure_h

#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <string>
//#include "SkipList.h"
//#include "HashTable.h"
//#include "Db.h"
#include "Tool.h"

namespace hybridKV {
    

class HashTable;
class hyDB;
class DB;
class SkipList;
class ThreadPool;


typedef uint32_t (*hashFunc)(const char*, size_t);
uint32_t smHasher(const char *key, size_t len);
uint32_t SuperFastHash (const char * data, size_t len);
class Config {
    
public:
    explicit Config(int);
    explicit Config();
    ~Config();
    void generateIndex();
    int slIndex(size_t n);
    
    uint32_t ht_size;
    uint32_t ht_seed;
    uint32_t ht_limits;
    uint32_t sl_size;
    uint32_t max_key;
    uint32_t min_key;
    hashFunc hasher;
    HashTable* lastDb;
    SkipList** sl_grp;
    ThreadPool* thrds;
    //    Timer* tmr;
    int* index;
    std::string name_;
    
};

}
#endif /* Configure_h */
