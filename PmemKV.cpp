// //
// //  PmemKV.cpp
// //  devl
// //
// //  Created by 吴海源 on 2018/12/12.
// //  Copyright © 2018 why. All rights reserved.
// //

// // PmemKV is a hybrid B+ tree, which inner nodes are in memory, leaf are in pmemory.
// // include file "BplusTree" and create pmemkv

// #include "PmemKV.h"

// namespace hybridKV {

// PmemKV::PmemKV(): tree_(new BplusTree) {
    
// }
// PmemKV::~PmemKV() {
    
// }
// int PmemKV::Get(const std::string &key, std::string *val) {
//     return tree_->Get(key, val);
// }
// int PmemKV::Put(const std::string &key, const std::string &val) {
//     return tree_->Put(key, val);
// }
// int PmemKV::Delete(const std::string &key) {
//     return tree_->Delete(key);
// }
// int PmemKV::Update(const std::string &key, const std::string &val) {
//     return tree_->Update(key, val);
// }
// int PmemKV::Scan(const std::string &beginKey, const std::string &lastKey, std::vector<std::string>* output) {
//     return tree_->Scan(beginKey, lastKey, output);
// }

// }
