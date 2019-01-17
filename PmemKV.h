//
//  PmemKV.h
//  devl
//
//  Created by 吴海源 on 2018/12/18.
//  Copyright © 2018 why. All rights reserved.
//

#ifndef PmemKV_h
#define PmemKV_h



// PmemKV is a hybrid B+ tree, which inner nodes are in memory, leaf are in pmemory.
// include file "BplusTree" and create pmemkv
#include <string>
#include <vector>
#include "DBInterface.h"
#include "BplusTree.h"

namespace hybridKV {
#ifdef HiKV_TEST
#else
class PmemKV : public hyDB {
public:
    PmemKV();
    ~PmemKV();
    
    int Get(const std::string& key, std::string* val) final;
    int Put(const std::string& key, const std::string& val) final;
    int Delete(const std::string& key) final;
    //int Scan(const std::string& beginKey, uint64_t n, std::vector<std::string>& output) final
    //{ return 0; }
#ifdef NEED_SCAN
    int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) final;
#endif
    int Update(const std::string& key, const std::string& val) final;
    void debug() {
        tree_->showAll();
    }
private:
    BplusTree* tree_;
};
    
#endif
    
}
#endif /* PmemKV_h */
