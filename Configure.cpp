//
//  Configure.cpp
//  devl
//
//  Created by 吴海源 on 2018/11/7.
//  Copyright © 2018 why. All rights reserved.
//

//#include <stdio.h>
#include <cstdint>
#include "Configure.h"

namespace hybridKV {
    
Config::Config(int size) : ht_size(1 << 30),
    ht_limits(1 << 31),
    ht_seed(5381),
    lastDb(nullptr),
    sl_size(size),
    max_key(128),
    min_key(8),
    hasher(smHasher),
    sl_grp(nullptr),
    thrds(nullptr),
    //        tmr(nullptr),
    index(new int [max_key+1])
     {
        LOG("gen index");
        generateIndex();
}
Config::Config() : ht_size(1 << 30),
    ht_limits(1 << 31),
   ht_seed(5381),
//        lastDb(nullptr),
//    sl_size(16),
//        max_key(128),
    hasher(smHasher),
//        sl_grp(nullptr),
    thrds(nullptr),
    //        tmr(nullptr),
//        index(new int [max_key]),
    name_("HiKV DB") {
        
}

Config::~Config() {
    if (index != nullptr)
        delete [] index;
    //    if (tmr != nullptr)
    //        delete tmr;
}
void Config::generateIndex() {

    int broad = (max_key-min_key) / sl_size;
    for (int i=min_key; i<max_key+1; ++i)
        index[i] = (i-min_key)/broad;
}


int Config::slIndex(size_t n) {
    if (n >= max_key)
        return sl_size - 1;
    return index[n];
}

#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

uint32_t SuperFastHash(const char * data, size_t len) {
    uint32_t hash = len, tmp;
    int rem;

    if (len <= 0 || data == NULL) return 0;

    rem = len & 3;
    len >>= 2;

    /* Main loop */
    for (;len > 0; len--) {
        hash  += get16bits (data);
        tmp    = (get16bits (data+2) << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        data  += 2*sizeof (uint16_t);
        hash  += hash >> 11;
    }

    /* Handle end cases */
    switch (rem) {
        case 3: hash += get16bits (data);
                hash ^= hash << 16;
                hash ^= ((signed char)data[sizeof (uint16_t)]) << 18;
                hash += hash >> 11;
                break;
        case 2: hash += get16bits (data);
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1: hash += (signed char)*data;
                hash ^= hash << 10;
                hash += hash >> 1;
    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}   
// An effcient Hash algorithm implemented by Austin Appleby.
// For more information at https://github.com/aappleby/smhasher.
uint32_t smHasher(const char *key, size_t len) {
    /* 'm' and 'r' are mixing constants generated offline.
     They're not really 'magic', they just happen to work well.  */
    // uint32_t seed = hash_seed;
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    uint32_t seed = 5381;
    /* Initialize the hash to a 'random' value */
    uint64_t h = seed ^ len;
    
    /* Mix 4 bytes at a time into the hash */
    const unsigned char *data = (const unsigned char *)key;
    
    while (len >= 4) {
        uint32_t k = *(uint32_t*)data;
        
        k *= m;
        k ^= k >> r;
        k *= m;
        
        h *= m;
        h ^= k;
        
        data += 4;
        len -= 4;
    }
    
    /* Handle the last few bytes of the input array  */
    switch(len) {
        case 3: h ^= data[2] << 16;
        case 2: h ^= data[1] << 8;
        case 1: h ^= data[0]; h *= m;
    };
    
    /* Do a few final mixes of the hash to ensure the last few
     * bytes are well-incorporated. */
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    
    return (uint32_t)h;
}
}


