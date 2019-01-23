#ifndef __KV_OBJ_H
#define __KV_OBJ_H

////////////////////////////////////////////////////////////////////////////////
//
// Basic Object class for Key or Value
// For now, only data address and size are used
// Other infos such as LRU, ref count etc can be added
// two usage:
// (1) Convert "void *" to "kvObj *" to access key/value data
// (2) Compare two kvObjs through size() and data() method
//
/////////////////////////////////////////////////////////////////////////////////
#include <memory>
#include <cstdlib>
#include <string>
#include <iostream>
#include <cstring>
#include <chrono>
#include "latency.hpp"
#include "hyKV_def.h"
#include "Timer.h"
namespace hybridKV {

class kvObj {

public:
    friend class HashTable;
    //when 'nvm_alloc' is true, NVM space need to be allocated to accommodate ‘Data’
    //else, just let it in DRAM
    kvObj(const std::string& s, bool nvm_alloc): size_(s.size()), nvm_allocated(nvm_alloc) {
        if (nvm_alloc) {
            char* buf = new char [size_+1];
            memcpy(buf, s.data(), size_);
            buf[size_] = '\0';
            data_ = buf;
        } else {
            data_ = s.data();
        }

    }
    
    kvObj(const char* dt, bool nvm_alloc): size_(strlen(dt)), nvm_allocated(nvm_alloc) {
        if (nvm_alloc) {
            char* buf = new char [size_+1];
            memcpy(buf, dt, size_);
            buf[size_] = '\0';
            data_ = buf;

        } else {
            data_ = dt;
        }
    }

    ~kvObj() {
        if (nvm_allocated)
            delete [] data_;
    }
    // inline void latency(bool op_write) const {
    //     if (nvm_allocated) {
    //         if (op_write) {
    //             pflush((uint64_t*)(data_), size_);
    //         } else {
    //             pload((uint64_t*)(data_), size_);
    //         }
    //     }
    // }
//    void addRef() { ++ref; }
    size_t size() const { return size_; }
    const char* data() const { return data_; }

    inline int compare(const kvObj& x) const {
        
// #ifdef PM_READ_LATENCY_TEST
//         x.latency(false);
// #endif
        uint64_t com_len = size_ < x.size()? size_ : x.size();
        int res = memcmp(data(), x.data(), com_len);
        if (res == 0) {
            if (size_ > x.size())
                res = 1;
            else if (size_ < x.size())
                res = -1;
        }
        return res;
    }

private:
	bool nvm_allocated;
    size_t size_;
//    uint32_t ref;
    const char* data_;

    kvObj() = delete;
    kvObj(const kvObj&);
    void operator=(const kvObj&);
};
    
inline bool operator==(const kvObj& x, const kvObj& y) {
#ifdef PM_READ_LATENCY_TEST
    y.latency(false);   // 'x' is not read from NVM. When it's created, we asume it is already in cpu cache.
#endif
    return (x.size() == y.size()) && (memcmp(x.data(), y.data(), x.size()) == 0);
}
inline bool operator!=(const kvObj& x, const kvObj& y) {
    return !(x==y);
}
inline bool operator<=(const kvObj& x, const kvObj& y) {
    return x.compare(y) <= 0;
}
inline bool operator<(const kvObj& x, const kvObj& y) {
    return x.compare(y) == -1;
}
inline int Compare(const kvObj& x, const kvObj& y) {
	return x.compare(y);
}
inline bool equal(const kvObj& x, const kvObj& y) {
	return (x == y);
}

}

#endif
