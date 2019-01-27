//
//  BplusTree.cpp
//  devl
//
//  Created by 吴海源 on 2018/12/18.
//  Copyright © 2018 why. All rights reserved.
//

#include "BplusTree.h"

namespace hybridKV {

void KVPairSplit::clear() {
    if (key_ != nullptr)
        delete key_;
    if (val_ != nullptr)
        delete val_;
    hash_ = 0;
    key_ = nullptr;
    val_ = nullptr;
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)(this), sizeof(KVPair));
#endif
#endif
}

void KVPairSplit::Set(const uint8_t hash, kvObj* key, kvObj* val) {
    hash_ = hash;
    key_ = key;
    val_ = val;
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)(this), sizeof(KVPair));
#endif
#endif
}
    
void KVPair::clear() {
    if (kv) delete [] kv;
    hash_ = 0;
    ksize_ = 0;
    vsize_ = 0;
    kv = nullptr;
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)(this), sizeof(KVPair));
#endif
#endif
}
void KVPair::Set(const uint8_t hash, const std::string& key, const std::string& value) {
    if (kv) delete [] kv;
    hash_ = hash;
    ksize_ = (uint32_t)key.size();
    vsize_ = (uint32_t)value.size();
    kv = new char[ksize_ + vsize_ + 2];
    char* ch = kv;
    memcpy(ch, key.data(), ksize_);
    kv[ksize_] = '\0';
    ch += ksize_+1;
    memcpy(ch, value.data(), vsize_);
    kv[ksize_ + vsize_ + 1] = '\0';
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
    pflush((uint64_t*)(kv), ksize_+vsize_+1);
    pflush((uint64_t*)(this), sizeof(KVPair));
#endif
#endif
}
// methods of BplusTree
    
int BplusTreeSplit::Insert(kvObj* k, kvObj* v) {
    // std::string tmp_key(key);
    // LOG("Ins");
    ++writeCnt;
    const char* key = k->data();
    size_t size = k->size();
    const uint8_t hash = PearsonHash(key, size);
    auto leafnode = leafSearch(key);
    
    if (!leafnode) {    // need a new leafnode to accommodate kv pair
        assert(head == nullptr);
        auto new_node = std::make_unique<KVLeafNodeSplit>();
        new_node->isLeaf = true;
        
        auto new_leaf = std::make_shared<KVLeafSplit>();
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(new_leaf.get()), sizeof(KVLeafSplit));
#endif
#endif
        // since there is no existing leafnode, just replace the head and mark its inner node the root.
        auto old_head = head;
        head = new_leaf;
        new_leaf->next = old_head.get();
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(&head), sizeof(void*));
        pflush((uint64_t*)(&new_leaf->next), sizeof(void*));
#endif    
#endif
        new_node->leaf = new_leaf;

        leafFillSpecificSlot(new_node.get(), hash, k, v, 0);
        
        root = std::move(new_node);
        
    } else if (leafFillSlotForKey(leafnode, hash, k, v)) { // there is already an existing "RangeMatched" leafnode
        
    } else {    // the "RangeMatched" leafnode is full, we need to split it
        leafSplitFull(leafnode, hash, k, v);
    }
    return 0;
}

int BplusTreeSplit::Update(kvObj* key, kvObj* val) {
    ++updateCnt;
    // LOG("Up");
    return Insert(key, val);
}
void BplusTreeSplit::leafFillEmptySlot(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val) {
    for (int slot = LEAF_KEYS; slot--;) {
        if (leafnode->hashes[slot] == 0) {
            leafFillSpecificSlot(leafnode, hash, key, val, slot);
            return;
        }
    }
}

    
void BplusTreeSplit::leafFillSpecificSlot(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val, const int slot) {
    if (leafnode->hashes[slot] == 0) {
        leafnode->hashes[slot] = hash;
        leafnode->keys[slot] = std::string(key->data());
    }
    leafnode->leaf->slots[slot]->Set(hash, key, val);
}
// void BplusTreeSplit::leafUpdateSpecificSlot(KVLeafNodeSplit* leafnode, kvObj* val, int slot) {
//     leafnode->leaf->slots[slot]->Set(hash, key, val);
// }
    
bool BplusTreeSplit::leafFillSlotForKey(KVLeafNodeSplit* leafnode, const uint8_t hash, kvObj* key, kvObj* val) {
    int last_empty_slot = -1;
    int key_match_slot = -1;
    // std::string tmp_key(key);
    for (int slot = LEAF_KEYS; slot--;) {
        auto slot_hash = leafnode->hashes[slot];
        if (slot_hash == 0) {
            last_empty_slot = slot;
        } else if (slot_hash == hash) {
             // ++leafCmpCnt;
            if (strcmp(leafnode->keys[slot].c_str(), key->data()) == 0) {
                key_match_slot = slot;
                // ++dupKeyCnt;
                break;
            }
        }
    }
    // if (key_match_slot >= 0) {
    //     leafUpdateSpecificSlot(leafnode, val, key_match_slot);
    //     return true;
    // } else {
    //     if (last_empty_slot >= 0) {
    //         leafFillSpecificSlot(leafnode, hash, key, val, last_empty_slot);
    //         return true;
    //     }
    // }
    // return false;
    if (key_match_slot >= 0)
        ++dupKeyCnt;
    int slot = key_match_slot>=0 ? key_match_slot : last_empty_slot;
    if (slot >= 0) {
        leafFillSpecificSlot(leafnode, hash, key, val, slot);
    }
    return slot >= 0;
}
    
void BplusTreeSplit::leafSplitFull(KVLeafNodeSplit *leafnode, const uint8_t hash, kvObj* skey, kvObj* val) {
    // ++leafSplitCnt;
    // ++leafCnt;
    // std::string key(src_key);
    std::string keys[LEAF_KEYS+1];
    keys[LEAF_KEYS] = std::string(skey->data());
    for (int slot = LEAF_KEYS; slot--;)
        keys[slot] = leafnode->keys[slot];
    auto cmp = [](const std::string& ls, const std::string& rs) {
        return (strcmp(ls.c_str(), rs.c_str()) < 0);
    };
    std::sort(std::begin(keys), std::end(keys), cmp);
    
    std::string split_key = keys[LEAF_KEYS_MIDPOINT];
    
    std::unique_ptr<KVLeafNodeSplit> new_leafnode(new KVLeafNodeSplit());
    new_leafnode->parent = leafnode->parent;
    new_leafnode->isLeaf = true;
    
    
    {
        //        std::shared_ptr<KVLeaf> new_leaf(new KVLeaf());
        auto new_leaf = std::make_shared<KVLeafSplit>();
#ifndef HiKV_TEST
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(new_leaf.get()), sizeof(KVLeafSplit));
#endif
#endif
        
        auto next_leaf = leafnode->leaf->next;
        auto next_leafnode = leafnode->next;
        
        new_leafnode->next = next_leafnode;
        leafnode->leaf->next = new_leaf.get();
        
        new_leaf->next = next_leaf;
        leafnode->next = new_leafnode.get();
        
#ifndef HiKV_TEST  
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(&new_leaf->next), sizeof(void*));
#endif
#endif

        new_leafnode->leaf = new_leaf;
        
        for (int slot = LEAF_KEYS; slot--;) {
            if (strcmp(leafnode->keys[slot].c_str(), split_key.data()) > 0) {
                // ++leafSplitCmp;
                new_leaf->slots[slot].swap(leafnode->leaf->slots[slot]);
                new_leafnode->hashes[slot] = leafnode->hashes[slot];
                new_leafnode->keys[slot] = leafnode->keys[slot];
                
                leafnode->hashes[slot] = 0;
                leafnode->keys[slot].clear();
            }
        }
        auto target = strcmp(skey->data(), split_key.data())>0?new_leafnode.get():leafnode;
        leafFillEmptySlot(target, hash, skey, val);
    }
    
    innerUpdateAfterSplit(leafnode, std::move(new_leafnode), &split_key);
    
}
// now we have two nodes(inner node or leaf node), old one and new one.
// the new one is split from the old one
// so the new one must has a inner node to index it
void BplusTreeSplit::innerUpdateAfterSplit(KVNodeSplit* node, std::unique_ptr<KVNodeSplit> new_node, std::string* split_key) {
    // no inner node yet
    // ++innerUpdateCnt;
    if (!node->parent) {
        auto top = std::make_unique<KVInnerNodeSplit>();
        top->keyCount = 1;
        top->keys[0] = *split_key;
        node->parent = top.get();
        new_node->parent = top.get();
        top->children[0] = std::move(root);
        top->children[1] = std::move(new_node);
        
        root = std::move(top);
        return;
    }
    KVInnerNodeSplit* inner = node->parent;
    {
        const uint8_t keycount = inner->keyCount;
        int idx = 0;
        while (idx < keycount && inner->keys[idx].compare(*split_key) <=0) ++idx;
        for (int i=keycount-1; i>=idx; --i) inner->keys[i+1] = std::move(inner->keys[i]);
        for (int i=keycount; i>idx; --i) inner->children[i+1] = std::move(inner->children[i]);
        inner->keys[idx] = *split_key;
        inner->children[idx+1] = std::move(new_node);
        inner->keyCount = (uint8_t)(keycount+1);
    }
    const uint8_t keycount = inner->keyCount;
    if (keycount <= INNER_KEYS)
        return;
    
    // this inner node is full either, split it, and update parents if necessary
    auto new_inner = std::make_unique<KVInnerNodeSplit>();
    new_inner->parent = inner->parent;
    for (int i=INNER_KEYS_UPPER; i<keycount; ++i) {
        new_inner->keys[i-INNER_KEYS_UPPER] = std::move(inner->keys[i]);
    }
    for (int i=INNER_KEYS_UPPER; i<keycount+1; ++i) {
        new_inner->children[i-INNER_KEYS_UPPER] = std::move(inner->children[i]);
        new_inner->children[i-INNER_KEYS_UPPER]->parent = new_inner.get();
    }
    new_inner->keyCount = INNER_KEYS_MIDPOINT;
    std::string new_split_key = inner->keys[INNER_KEYS_MIDPOINT];
    inner->keyCount = INNER_KEYS_MIDPOINT;
    
    innerUpdateAfterSplit(inner, std::move(new_inner), &new_split_key);
}

KVLeafNodeSplit* BplusTreeSplit::leafSearch(const char* key) {
    // ++leafSearchCnt;
    KVNodeSplit* node = root.get();
    if (node == nullptr) return nullptr;
    bool matched;
    // int height = 0;
    while (!node->isLeaf) {
        // ++height;
        // ++logCmpCnt;
        matched = false;
        auto inner = (KVInnerNodeSplit*) node;
        const uint8_t keycount = inner->keyCount;
        // tmr_log.start();
        
        // auto lower = std::lower_bound(inner->keys.begin(), inner->keys.begin()+keycount, key);
        // int dist = std::distance(inner->keys.begin(), lower);
        // node = inner->children[dist].get();
        for (uint8_t idx = 0; idx<keycount; ++idx) {
            node = inner->children[idx].get();
            if (strcmp(key, inner->keys[idx].c_str()) <= 0) {
                matched = true;
                break;
            }
        }
        if (!matched) node = inner->children[keycount].get();
        // tmr_log.stop();
        
    }
    // if (depth < height)
    //     depth = height;
    return (KVLeafNodeSplit*) node;
}
int BplusTreeSplit::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) {
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    ++scanCnt;
    auto leafnode = leafSearch(beginKey.c_str());
    while (leafnode) {
        std::vector<std::pair<std::string*, int>> tmp_r;
        for (int slot = LEAF_KEYS; slot--;) {
            if (leafnode->hashes[slot] != 0)
                tmp_r.push_back({&leafnode->keys[slot], slot});
        }
        auto cmp = [](std::pair<std::string*, int> &a, std::pair<std::string*, int> &b) {
            return *a.first < *b.first;
        };
        std::sort(tmp_r.begin(), tmp_r.end(), cmp);
        
        bool finished = false;
        auto itor = tmp_r.begin();
        while (itor < tmp_r.end() && *itor->first < beginKey) ++itor;
        while (itor < tmp_r.end()) {
            finished = false;
            if(*itor->first <= lastKey) {
                output.push_back(std::string(leafnode->leaf->slots[itor->second]->val()));
#ifdef PM_READ_LATENCY_TEST
                pload((uint64_t*)(leafnode->leaf->slots[itor->second]->val()), leafnode->leaf->slots[itor->second]->valsize());
#endif
            } else {
                finished = true; break;
            }
            ++itor;
        }
        leafnode = finished? nullptr : leafnode->next;
        
    }
    return 0;
}

int BplusTreeSplit::Scan(kvObj* beginKey, kvObj* lastKey, std::vector<std::string>* output) {
    ++scanCnt;
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    auto leafnode = leafSearch(beginKey->data());
    while (leafnode) {
        std::vector<std::pair<std::string*, int>> tmp_r;
        for (int slot = LEAF_KEYS; slot--;) {
            if (leafnode->hashes[slot] != 0)
                tmp_r.push_back({&leafnode->keys[slot], slot});
        }
        auto cmp = [](std::pair<std::string*, int> &a, std::pair<std::string*, int> &b) {
            return *a.first < *b.first;
        };
        std::sort(tmp_r.begin(), tmp_r.end(), cmp);
        
        bool finished = false;
        auto itor = tmp_r.begin();
        while (itor < tmp_r.end() && strcmp(itor->first->c_str(), beginKey->data()) < 0) ++itor;
        while (itor < tmp_r.end()) {
            finished = false;
            if(strcmp(itor->first->c_str(), lastKey->data()) <= 0) {
                output->push_back(std::string(leafnode->leaf->slots[itor->second]->val()));
#ifdef PM_READ_LATENCY_TEST
                pload((uint64_t*)(leafnode->leaf->slots[itor->second]->val()), leafnode->leaf->slots[itor->second]->valsize());
#endif
            } else {
                finished = true; break;
            }
            ++itor;
        }
        leafnode = finished? nullptr : leafnode->next;
        
    }
    return 0;
}
int BplusTreeSplit::Delete(kvObj* k) {
    // LOG("Delete Key" << key.c_str());
    ++delCnt;
    const char* key = k->data();
    auto leafnode = leafSearch(key);
    
    if (!leafnode) {
        return -1;
    }
    const uint8_t hash = PearsonHash(key, k->size());
    for (int slot = LEAF_KEYS; slot--;) {
        if (leafnode->hashes[slot] == hash) {
            if (strcmp(leafnode->keys[slot].c_str(), key) == 0) {
                leafnode->hashes[slot] = 0;
                leafnode->keys[slot].clear();
                auto leaf = leafnode->leaf;
                leaf->slots[slot]->clear();
            }
            break;
        }
    }
    return 0;
}

int BplusTreeSplit::Delete(const std::string& key) {
    ++delCnt;
    // LOG("Delete Key" << key.c_str());
    auto leafnode = leafSearch(key.c_str());
    
    if (!leafnode) {
        return -1;
    }
    const uint8_t hash = PearsonHash(key.c_str(), key.size());
    for (int slot = LEAF_KEYS; slot--;) {
        if (leafnode->hashes[slot] == hash) {
            if (strcmp(leafnode->keys[slot].c_str(), key.c_str()) == 0) {
                leafnode->hashes[slot] = 0;
                leafnode->keys[slot].clear();
                auto leaf = leafnode->leaf;
                leaf->slots[slot]->clear();
            }
            break;
        }
    }
    return 0;
}

int BplusTreeSplit::Get(const std::string& key, std::string* val) {
//    LOG("Get Key " << key.c_str());
    auto leafnode = leafSearch(key.c_str());
    if (leafnode) {
        const uint8_t hash = PearsonHash(key.c_str(), key.size());
        for (int slot = LEAF_KEYS; slot--;) {
            if (leafnode->hashes[slot] == hash) {
                if (strcmp(leafnode->keys[slot].c_str(), key.c_str()) == 0) {
                    auto kv = leafnode->leaf->slots[slot].get();
                    val->append(kv->val());
#ifndef HiKV_TEST
#ifdef PM_READ_LATENCY_TEST
                    pload((uint64_t*)(kv), sizeof(KVPair));
                    pload((uint64_t*)(kv->val()), (size_t)kv->valsize());
#endif
#endif
                    return 0;
                }
            }
        }
    }
    // ++notFoundCnt;
    return -1;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////



int BplusTree::Put(const std::string& key, const std::string& val) {
    //LOG("Put: Key "<< key.c_str() << ". value " << val.c_str());
    const uint8_t hash = PearsonHash(key.c_str(), key.size());
    // tmr_search.start();
    auto leafnode = leafSearch(key);
    // tmr_search.stop();
    // As far as I think, leafnode is equal to nullptr only when there is no node at all.
    // Otherwise, leafnode can't be nullptr.
    if (!leafnode) {    // need a new leafnode to accommodate kv pair
        // ++leafCnt;
        assert(head == nullptr);
        auto new_node = std::make_unique<KVLeafNode>();
        new_node->isLeaf = true;
        
        //        std::shared_ptr<KVLeaf> new_leaf(new KVLeaf());
        auto new_leaf = std::make_shared<KVLeaf>();
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(new_leaf.get()), sizeof(KVLeaf));
#endif
        // since there is no existing leafnode, just replace the head and mark its inner node the root.
        auto old_head = head;
        head = new_leaf;
        new_leaf->next = old_head.get();
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(&head), sizeof(void*));
        pflush((uint64_t*)(&new_leaf->next), sizeof(void*));
#endif
        new_node->leaf = new_leaf;
        
        leafFillSpecificSlot(new_node.get(), hash, key, val, 0);
        
        root = std::move(new_node); // root is not supported to be persistent, because it can be rebuilt via "::Recovery()"
        
    } else if (leafFillSlotForKey(leafnode, hash, key, val)) { // there is already an existing "RangeMatched" leafnode
        
    } else {    // the "RangeMatched" leafnode is full, we need to split it
        leafSplitFull(leafnode, hash, key, val);
    }
    return 0;
}
    
int BplusTree::Update(const std::string& key, const std::string& val) {
    return Put(key, val);
}
    
void BplusTree::leafFillEmptySlot(KVLeafNode* leafnode, const uint8_t hash, const std::string& key, const std::string& val) {
    for (int slot = LEAF_KEYS; slot--;) {
        if (leafnode->hashes[slot] == 0) {
            leafFillSpecificSlot(leafnode, hash, key, val, slot);
            return;
        }
    }
}
    
void BplusTree::leafFillSpecificSlot(KVLeafNode *leafnode, const uint8_t hash, const std::string& key, const std::string& val, const int slot) {
    if (leafnode->hashes[slot] == 0) {
        leafnode->hashes[slot] = hash;
        leafnode->keys[slot] = key;
    }
    leafnode->leaf->slots[slot]->Set(hash, key, val);
}

bool BplusTree::leafFillSlotForKey(KVLeafNode *leafnode, const uint8_t hash, const std::string& key, const std::string& val) {
    int last_empty_slot = -1;
    int key_match_slot = -1;
    // tmr_insert.start();
    for (int slot = LEAF_KEYS; slot--;) {
        // ++leafCmpCnt;
        auto slot_hash = leafnode->hashes[slot];
        if (slot_hash == 0) {
            last_empty_slot = slot;
        } else if (slot_hash == hash) {
            if (strcmp(leafnode->keys[slot].c_str(), key.c_str()) == 0) {
                key_match_slot = slot;
                // ++dupKeyCnt;
                break;
            }
        }
    }
    int slot = key_match_slot>=0 ? key_match_slot : last_empty_slot;
    if (slot >= 0) {
        leafFillSpecificSlot(leafnode, hash, key, val, slot);
    }
    // tmr_insert.stop();
    return slot >= 0;
}


void BplusTree::leafSplitFull(KVLeafNode *leafnode, const uint8_t hash, const std::string &key, const std::string &val) {
    // tmr_split.start();
    // ++leafSplitCnt;
    // ++leafCnt;
    std::string keys[LEAF_KEYS+1];
    keys[LEAF_KEYS] = key;
    for (int slot = LEAF_KEYS; slot--;)
        keys[slot] = leafnode->keys[slot];
    auto cmp = [](const std::string& ls, const std::string& rs) {
        return (strcmp(ls.c_str(), rs.c_str()) < 0);
    };
    std::sort(std::begin(keys), std::end(keys), cmp);
    
    std::string split_key = keys[LEAF_KEYS_MIDPOINT];
    
    std::unique_ptr<KVLeafNode> new_leafnode(new KVLeafNode());
    new_leafnode->parent = leafnode->parent;
    new_leafnode->isLeaf = true;
    
    
    {
        //        std::shared_ptr<KVLeaf> new_leaf(new KVLeaf());
        auto new_leaf = std::make_shared<KVLeaf>();
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(new_leaf.get()), sizeof(KVLeaf));
#endif
        
        auto next_leaf = leafnode->leaf->next;
        auto next_leafnode = leafnode->next;
        
        new_leafnode->next = next_leafnode;
        leafnode->leaf->next = new_leaf.get();
        
        new_leaf->next = next_leaf;
        leafnode->next = new_leafnode.get();
        
        
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(&new_leaf->next), sizeof(void*));
#endif
        new_leafnode->leaf = new_leaf;
        
        for (int slot = LEAF_KEYS; slot--;) {
            if (strcmp(leafnode->keys[slot].c_str(), split_key.data()) > 0) {
                ++leafSplitCmp;
                new_leaf->slots[slot].swap(leafnode->leaf->slots[slot]);
                new_leafnode->hashes[slot] = leafnode->hashes[slot];
                new_leafnode->keys[slot] = leafnode->keys[slot];
                
                leafnode->hashes[slot] = 0;
                leafnode->keys[slot].clear();
            }
        }
        auto target = strcmp(key.c_str(), split_key.data())>0?new_leafnode.get():leafnode;
        leafFillEmptySlot(target, hash, key, val);
    }
    // tmr_split.stop();

    // tmr_inner.start();
    innerUpdateAfterSplit(leafnode, std::move(new_leafnode), &split_key);
    // tmr_inner.stop();
}
// #endif
// now we have two nodes(inner node or leaf node), old one and new one.
// the new one is split from the old one
// so the new one must has a inner node to index it
void BplusTree::innerUpdateAfterSplit(KVNode* node, std::unique_ptr<KVNode> new_node, std::string* split_key) {
    // no inner node yet
    // ++innerUpdateCnt;
    if (!node->parent) {
        auto top = std::make_unique<KVInnerNode>();
        top->keyCount = 1;
        top->keys[0] = *split_key;
        node->parent = top.get();
        new_node->parent = top.get();
        top->children[0] = std::move(root);
        top->children[1] = std::move(new_node);
        
        root = std::move(top);
        return;
    }
    KVInnerNode* inner = node->parent;
    {
        const uint8_t keycount = inner->keyCount;
        int idx = 0;
        while (idx < keycount && inner->keys[idx].compare(*split_key) <=0) ++idx;
        for (int i=keycount-1; i>=idx; --i) inner->keys[i+1] = std::move(inner->keys[i]);
        for (int i=keycount; i>idx; --i) inner->children[i+1] = std::move(inner->children[i]);
        inner->keys[idx] = *split_key;
        inner->children[idx+1] = std::move(new_node);
        inner->keyCount = (uint8_t)(keycount+1);
    }
    const uint8_t keycount = inner->keyCount;
    if (keycount <= INNER_KEYS)
        return;
    
    // this inner node is full either, split it, and update parents if necessary
    auto new_inner = std::make_unique<KVInnerNode>();
    new_inner->parent = inner->parent;
    for (int i=INNER_KEYS_UPPER; i<keycount; ++i) {
        new_inner->keys[i-INNER_KEYS_UPPER] = std::move(inner->keys[i]);
    }
    for (int i=INNER_KEYS_UPPER; i<keycount+1; ++i) {
        new_inner->children[i-INNER_KEYS_UPPER] = std::move(inner->children[i]);
        new_inner->children[i-INNER_KEYS_UPPER]->parent = new_inner.get();
    }
    new_inner->keyCount = INNER_KEYS_MIDPOINT;
    std::string new_split_key = inner->keys[INNER_KEYS_MIDPOINT];
    inner->keyCount = INNER_KEYS_MIDPOINT;
    
    innerUpdateAfterSplit(inner, std::move(new_inner), &new_split_key);
}

KVLeafNode* BplusTree::leafSearch(const std::string &key) {
    // ++leafSearchCnt;
    KVNode* node = root.get();
    if (node == nullptr) return nullptr;
    bool matched;
    // int height = 0;
    while (!node->isLeaf) {
        // ++height;
        // ++logCmpCnt;
        matched = false;
        auto inner = (KVInnerNode*) node;
        const uint8_t keycount = inner->keyCount;
        // tmr_log.start();
        
        // auto lower = std::lower_bound(inner->keys.begin(), inner->keys.begin()+keycount, key);
        // int dist = std::distance(inner->keys.begin(), lower);
        // node = inner->children[dist].get();
        for (uint8_t idx = 0; idx<keycount; ++idx) {
            node = inner->children[idx].get();
            if (strcmp(key.c_str(), inner->keys[idx].c_str()) <= 0) {
                matched = true;
                break;
            }
        }
        if (!matched) node = inner->children[keycount].get();
        // tmr_log.stop();
        
    }
    // if (depth < height)
    //     depth = height;
    return (KVLeafNode*) node;
}

// #ifdef NEED_SCAN
// #ifdef HiKV_TEST
// int BplusTree::Scan(const std::string& beginKey, const std::string& lastKey, scanRes& output) {
//     LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
//     auto leafnode = leafSearch(beginKey);
//     while (leafnode) {
//         std::vector<std::pair<std::string, int>> tmp_r;
//         for (int slot = LEAF_KEYS; slot--;) {
//             if (leafnode->hashes[slot] != 0)
//                 tmp_r.push_back({leafnode->keys[slot], slot});
//         }
//         auto cmp = [](std::pair<std::string, int> &a, std::pair<std::string, int> &b) {
//             return a.first < b.first;
//         };
//         std::sort(tmp_r.begin(), tmp_r.end(), cmp);
        
//         bool finished = false;
//         auto itor = tmp_r.begin();
//         while (itor < tmp_r.end() && itor->first < beginKey) ++itor;
//         while (itor < tmp_r.end()) {
//             finished = false;
//             if(itor->first <= lastKey) {
//                 output.elems.push_back((void*)(leafnode->leaf->slots[itor->second]->key()));
//             } else {
//                 finished = true; break;
//             }
//             ++itor;
//         }
//         leafnode = finished? nullptr : leafnode->next;
        
//     }
//     output.done.Release_Store(reinterpret_cast<void*>(1));
//     return 0;
// }
// #else
int BplusTree::Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) {
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    auto leafnode = leafSearch(beginKey);
    while (leafnode) {
        std::vector<std::pair<std::string*, int>> tmp_r;
        for (int slot = LEAF_KEYS; slot--;) {
            if (leafnode->hashes[slot] != 0)
                tmp_r.push_back({&leafnode->keys[slot], slot});
        }
        auto cmp = [](std::pair<std::string*, int> &a, std::pair<std::string*, int> &b) {
            return *a.first < *b.first;
        };
        std::sort(tmp_r.begin(), tmp_r.end(), cmp);
        
        bool finished = false;
        auto itor = tmp_r.begin();
        while (itor < tmp_r.end() && *itor->first < beginKey) ++itor;
        while (itor < tmp_r.end()) {
            finished = false;
            if(*itor->first <= lastKey) {
                output.push_back(std::string(leafnode->leaf->slots[itor->second]->val()));
#ifdef PM_READ_LATENCY_TEST
                pload((uint64_t*)(leafnode->leaf->slots[itor->second]->val()), leafnode->leaf->slots[itor->second]->valsize());
#endif
            } else {
                finished = true; break;
            }
            ++itor;
        }
        leafnode = finished? nullptr : leafnode->next;
        
    }
    return 0;
}
// #endif
// #endif
int BplusTree::Delete(const std::string& key) {
    // LOG("Delete Key" << key.c_str());
    auto leafnode = leafSearch(key);
    
    if (!leafnode) {
        // LOG("NO such Key");
        return -1;
    }
    const uint8_t hash = PearsonHash(key.c_str(), key.size());
    for (int slot = LEAF_KEYS; slot--;) {
        if (leafnode->hashes[slot] == hash) {
            if (strcmp(leafnode->keys[slot].c_str(), key.c_str()) == 0) {
                leafnode->hashes[slot] = 0;
                leafnode->keys[slot].clear();
                auto leaf = leafnode->leaf;
                leaf->slots[slot]->clear();
            }
            break;
        }
    }
    return 0;
}

int BplusTree::Get(const std::string& key, std::string* val) {
//    LOG("Get Key " << key.c_str());
    auto leafnode = leafSearch(key);
    if (leafnode) {
        const uint8_t hash = PearsonHash(key.c_str(), key.size());
        for (int slot = LEAF_KEYS; slot--;) {
            if (leafnode->hashes[slot] == hash) {
                if (strcmp(leafnode->keys[slot].c_str(), key.c_str()) == 0) {
                    auto kv = leafnode->leaf->slots[slot].get();
                    val->append(kv->val());
#ifdef PM_READ_LATENCY_TEST
                    pload((uint64_t*)(kv), sizeof(KVPair));
                    pload((uint64_t*)(kv->val()), (size_t)kv->valsize());
#endif
                    return 0;
                }
            }
        }
    }
    // ++notFoundCnt;
    return -1;
}


// Pearson hashing lookup table from RFC 3074
const uint8_t PEARSON_LOOKUP_TABLE[256] = {
    251, 175, 119, 215, 81, 14, 79, 191, 103, 49, 181, 143, 186, 157, 0,
    232, 31, 32, 55, 60, 152, 58, 17, 237, 174, 70, 160, 144, 220, 90, 57,
    223, 59, 3, 18, 140, 111, 166, 203, 196, 134, 243, 124, 95, 222, 179,
    197, 65, 180, 48, 36, 15, 107, 46, 233, 130, 165, 30, 123, 161, 209, 23,
    97, 16, 40, 91, 219, 61, 100, 10, 210, 109, 250, 127, 22, 138, 29, 108,
    244, 67, 207, 9, 178, 204, 74, 98, 126, 249, 167, 116, 34, 77, 193,
    200, 121, 5, 20, 113, 71, 35, 128, 13, 182, 94, 25, 226, 227, 199, 75,
    27, 41, 245, 230, 224, 43, 225, 177, 26, 155, 150, 212, 142, 218, 115,
    241, 73, 88, 105, 39, 114, 62, 255, 192, 201, 145, 214, 168, 158, 221,
    148, 154, 122, 12, 84, 82, 163, 44, 139, 228, 236, 205, 242, 217, 11,
    187, 146, 159, 64, 86, 239, 195, 42, 106, 198, 118, 112, 184, 172, 87,
    2, 173, 117, 176, 229, 247, 253, 137, 185, 99, 164, 102, 147, 45, 66,
    231, 52, 141, 211, 194, 206, 246, 238, 56, 110, 78, 248, 63, 240, 189,
    93, 92, 51, 53, 183, 19, 171, 72, 50, 33, 104, 101, 69, 8, 252, 83, 120,
    76, 135, 85, 54, 202, 125, 188, 213, 96, 235, 136, 208, 162, 129, 190,
    132, 156, 38, 47, 1, 7, 254, 24, 4, 216, 131, 89, 21, 28, 133, 37, 153,
    149, 80, 170, 68, 6, 169, 234, 151
};

// Modified Pearson hashing algorithm from RFC 3074
uint8_t BplusTree::PearsonHash(const char* data, const size_t size) {
    auto hash = (uint8_t) size;
    for (size_t i = size; i > 0;) {
        hash = PEARSON_LOOKUP_TABLE[hash ^ data[--i]];
    }
    // MODIFICATION START
    return (hash == 0) ? (uint8_t) 1 : hash;                             // 0 reserved for "null"
    // MODIFICATION END
}
uint8_t BplusTreeSplit::PearsonHash(const char* data, const size_t size) {
    auto hash = (uint8_t) size;
    for (size_t i = size; i > 0;) {
        hash = PEARSON_LOOKUP_TABLE[hash ^ data[--i]];
    }
    // MODIFICATION START
    return (hash == 0) ? (uint8_t) 1 : hash;                             // 0 reserved for "null"
    // MODIFICATION END
}
  
}
