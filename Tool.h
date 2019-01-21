#ifndef __TOOLS__H
#define __TOOLS__H

// informations used for recover and restart DB.
// Remember: skiplist and config are stored in memory, so we asume that the two 
//           pointers are always valid, and HashTable can be recovered via it.
#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <string>
#include <cstdint>
#include <list>
#include "atomic_pointer.h"

namespace hybridKV {

typedef signed char         int8_t;
typedef signed short        int16_t;
typedef signed int          int32_t;
typedef signed long long    int64_t;

typedef unsigned char       uint8_t;
typedef unsigned short      uint16_t;
typedef unsigned int        uint32_t;
typedef unsigned long       uint64_t;
    
const bool debug_on = true;
    
enum cmdType{
    kDeleteType = 0x0,
    kUpdateType = 0x1,
    kInsertType = 0x2,
    kScanNorType = 0x3,
    kScanNumType = 0x4,
    kFlushType = 0x5
};
typedef leveldb::port::AtomicPointer AtomicPointer;
class scanRes {
public:
    explicit scanRes() {
        done.Release_Store(reinterpret_cast<void*>(0));
        elems.clear();
    }
    AtomicPointer done;
    std::list<const char*> elems;
};
#define LOG(info) if(debug_on) std::cout<< info << std::endl
// void Latency(uint64_t time) {
	
// }


// class Timer {
// public:
//     clock_t Current();
//     clock_t Interval(clock_t a, clock_t b);

// private:
//     std::chrono
// };


}

#endif



