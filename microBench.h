//
//  microBench.h
//  devl
//
//  Created by 吴海源 on 2018/11/8.
//  Copyright © 2018 why. All rights reserved.
//

#ifndef microBench_h
#define microBench_h

#include <string>
#include <vector>
#include <random>
//#include <chrono>
#include <memory>
#include "Tool.h"
#include "Random.h"

namespace hybridKV {
    
const static int min_key = 8;
const static int max_key = 128;
const static int common_key = 32;

static const int min_val = 16;
static const int max_val = 1024;
static const int common_val = 256;

// Distribution Type:
//  kvNormal for  uniform
//  kvZipf   for  Zipf
//  kvReal   for  dist model in Paper "Workload Analysis for Large-Scale KV Store"
enum kvDistType {
    kvUniform = 0x0,
    kvZipf = 0x1,
    kvReal = 0x2
};

void generatorIndex() {
    
}

//class keyGenerator {
//public:
//    virtual void Next();
//    virtual
//private:
//
//};


std::string randomString(uint32_t len) {
    std::string res(len, ' ');
    for (int i=0; i<len; ++i)
        res[i] = static_cast<char>(' ' + (random() % 95));
    return res;
}

void generateScope(kvDistType type, std::vector<uint32_t>& minLen) {
    int jump = (max_key - min_key) / minLen.size();
    if (type == kvUniform){
        for (int i=0; i<minLen.size(); ++i) {
            minLen[i] = min_key + jump * i;
        }
    } else if (type == kvZipf) {
            
    } else if (type == kvReal) {
        
    }
}

    
// class uniformKeyGenerator {
// public:
//     uniformKeyGenerator(std::vector<uint32_t>& minLen) : rd(), gen(rd()), dist(min_key, max_key), rnd_(5331) {
//     }

//     std::string nextStr() {
//         int idx = dist(gen);
//         return randomString(&rnd_, idx);
//     }

// private:
//     int size;
//     Random rnd_;
//     std::random_device rd;
//     std::mt19937 gen;
//     std::uniform_int_distribution<> dist;
// };
    
class uniformKeyGenerator {
public:
    uniformKeyGenerator(std::vector<uint32_t>& minLen) : size((int)minLen.size()), rd(), gen(rd()), dist(0, size-1), dist_size(min_key, max_key), rnd_(5331), maxKey(size, 0), prefixStr(size) {
        for (auto i=0; i<size; ++i)
            prefixStr[i] = randomString(minLen[i]);
    }
    int nextLen() {
        return dist_size(gen);
    }
    std::string nextStr() {
//        std::string res;
        int idx = dist(gen);
//        LOG("idx = " << idx << " size" << prefixStr.size());
        ++maxKey[idx];
        return prefixStr[idx] + std::to_string(maxKey[idx]-1);
    }
    std::string randomReadStr() {
        int idx = dist(gen);
        return prefixStr[idx] + std::to_string(rnd_.Uniform(maxKey[idx]));
    }
private:
    int size;
    Random rnd_;
    std::random_device rd;
    std::mt19937 gen;
    std::uniform_int_distribution<> dist;
    std::uniform_int_distribution<> dist_size;
    std::vector<uint32_t> maxKey;
    std::vector<std::string> prefixStr;
};

class gevKeyGenerator {
public:
    gevKeyGenerator() : rnd_(5301), rd(), gen(rd()), dist(min_key, max_key), prefixStr(max_key/8+1, "") {
        prefixStr[0] = std::string("abcd");  
        for (int keyLen = min_key; keyLen < max_key; keyLen+=8)
            prefixStr[keyLen/8] = randomString(keyLen);
    }
    std::string nextStr() {
        int keylen = dist(gen);
        // LOG(keylen);
        assert(keylen >= min_key);
        if (keylen < 8)
            return randomString(keylen);
        if (keylen < 16)
            return prefixStr[0] + randomString(keylen % 8 + 4);

        return prefixStr[keylen/8-1] + randomString(keylen % 8 + 8);

    }
private:
    Random rnd_;
    std::random_device rd;
    std::mt19937 gen;
    std::uniform_int_distribution<> dist;
    std::vector<std::string> prefixStr;
};
    
}
#endif /* microBench_h */

