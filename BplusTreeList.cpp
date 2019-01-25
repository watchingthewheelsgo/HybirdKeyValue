//
//  BplusTree.cpp
//  devl
//
//  Created by 吴海源 on 2018/12/18.
//  Copyright © 2018 why. All rights reserved.
//

#include "BplusTreeList.h"

namespace hybridKV {

int BplusTreeList::Insert(KVPairFin* kv) {
    // std::string tmp_key(key);
    const uint8_t hash = PearsonHash(kv->key(), kv->keysize());
    tmr_search.start();
    auto leafnode = leafSearch(kv->key());
    tmr_search.stop();
    kv->setHash(hash);
    if (!leafnode) {    // need a new leafnode to accommodate kv pair
        ++leafCnt;
        assert(head == nullptr);
        auto new_node = std::make_unique<KVLeafNodeFin>();
        new_node->isLeaf = true;
        
        //        std::shared_ptr<KVLeaf> new_leaf(new KVLeaf());
        auto new_leaf = std::make_shared<KVLeafFin>();
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(new_leaf.get()), sizeof(KVLeafFin));
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

        new_leaf->lst.push_back(kv);

        new_node->lst.push_back(KVCachedNodeFin(hash, std::string(kv->key()), new_leaf->lst.begin()));
        // leafFillSpecificSlot(new_node.get(), hash, kv, 0);
        ++new_node->cnt;
        root = std::move(new_node);
        
    } else if (leafFillSlotForKey(leafnode, hash, kv)) { // there is already an existing "RangeMatched" leafnode
        
    } else {    // the "RangeMatched" leafnode is full, we need to split it
        leafSplitFull(leafnode, kv);
    }
    return 0;
}

bool BplusTreeList::leafFillSlotForKey(KVLeafNodeFin *leafnode, uint8_t hash, KVPairFin* kv) {
    bool flag = false;
    tmr_insert.start();
    if (strcmp(leafnode->lst.back().key.c_str(), kv->key()) < 0) {
        ++leafnode->cnt;
        tmr_cache.start();
        auto itor = leafnode->leaf->lst.insert(leafnode->leaf->lst.end(), kv);
        leafnode->lst.push_back(KVCachedNodeFin(hash, std::string(kv->key()), itor));
        tmr_cache.stop();
        ++pushBackCnt;
        flag = true;
    } else {
        for (auto itor=leafnode->lst.begin(); itor != leafnode->lst.end(); ++itor) {
            ++leafCmpCnt;
            int m =memcmp((*itor).key.c_str(), kv->key(), kv->keysize());
            if (m == 0) {
                 ++dupKeyCnt;
                (*itor->ptr)->Update(kv->getVal());
#ifdef PM_WRITE_LATENCY_TEST
                pflush((uint64_t*)((*itor->ptr)->getVal()), sizeof(void*));
#endif
                kv->freeKey();
                delete kv; // the KVPairFin is no longer needed.
                tmr_insert.stop();
                return true;
            } else if (m > 0) {
                ++leafnode->cnt;
                tmr_cache.start();
                auto tmp = leafnode->leaf->lst.insert(itor->ptr, kv);
#ifdef PM_WRITE_LATENCY_TEST
                // 2 changed pointers for the list need to be persistent.
                // It's hard to call pflush on std::list<>::iterator, so we just emulate the latency here.
                // note we can implemet our own double-linked list to make it easier to pflush pointers.
                // But, why not std::list and emulate write latency, right?
                emulate_latency_ns(800); 
#endif
                leafnode->lst.insert(itor, KVCachedNodeFin(hash, std::string(kv->key()), tmp));
                tmr_cache.stop();
                flag = true;
                //return true;
                break;
            } 
        }
    }
    tmr_insert.stop();
    if (!flag)
        ++skewLeafCnt;
    if (leafnode->cnt == LEAF_KEYS_L+1) {
        return false;
    }
    return true;
}
    
void BplusTreeList::leafSplitFull(KVLeafNodeFin *leafnode, KVPairFin* kv) {
    tmr_split.start();
    ++leafSplitCnt;
    ++leafCnt;
    auto mid = leafnode->lst.begin();
    std::advance(mid, LEAF_KEYS_MIDPOINT_L);
    std::string split_key = mid->key;
    std::advance(mid, 1);

    std::unique_ptr<KVLeafNodeFin> new_leafnode(new KVLeafNodeFin());
    new_leafnode->parent = leafnode->parent;
    new_leafnode->isLeaf = true;
    
    
    {
        //        std::shared_ptr<KVLeaf> new_leaf(new KVLeaf());
        auto new_leaf = std::make_shared<KVLeafFin>();
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)(new_leaf.get()), sizeof(KVLeafFin));
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
        
        new_leaf->lst.splice(new_leaf->lst.begin(), leafnode->leaf->lst, mid->ptr, leafnode->leaf->lst.end());
#ifdef PM_WRITE_LATENCY_TEST
        emulate_latency_ns(1600);
#endif

        new_leafnode->lst.splice(new_leafnode->lst.begin(), leafnode->lst, mid, leafnode->lst.end());
        leafnode->cnt = LEAF_KEYS_MIDPOINT_L+1;
        new_leafnode->cnt = LEAF_KEYS_MIDPOINT_L;
    
    }
    tmr_split.stop();
    
    tmr_inner.start();
    innerUpdateAfterSplit(leafnode, std::move(new_leafnode), &split_key);
    tmr_inner.stop();
}
// test use
void BplusTreeList::Geo() {
    int max=0, min = LEAF_KEYS_L;
    int sum = 0;
    int leafcnt;
    auto itor = head.get();
    while (itor != nullptr) {
        int x = itor->lst.size();
        max = std::max(max, x);
        min = std::min(min, x);
        sum += x;
        itor = itor->next;
    }
    LOG("There are " << leafcnt << " leafs. " << "Most "<< max << " nodes, Min " << min << " nodes. Sum is " << sum);
}
bool BplusTreeList::sortedLeaf() {
    auto itor = head.get();
    while (itor != nullptr) {
        auto tar = itor->lst;
        if (tar.empty())
            continue;
        auto f1 = tar.begin();
        auto f2 = f1;
        std::advance(f2, 1);
        while (f2 != tar.end()) {
            if (strcmp((*f1)->key(), (*f2)->key()) > 0)
                return false;
            std::advance(f1, 1);
            std::advance(f2, 1);
        }
        itor = itor->next;
    }
    return true;
}
void BplusTreeList::printLeafnode(const std::string& key) {
    auto leafnode = leafSearch(key.c_str());
    LOG("search for: " << key);
    for (auto &i:leafnode->lst) {
        LOG("key:"<< i.key);
    }
}
void BplusTreeList::printNoLeaf(int x) {
    auto itor = head.get();
    while (itor != nullptr && x-- != 0)  itor=itor->next;

    for (auto &i:itor->lst) {
        LOG("key:"<< i->key());
    }
    // auto parent = (KVInnerNodeFin*)itor->parent;
    // LOG("Piovt keys: ");
    // for (int i=0; i<parent->keyCount; ++i)
    //     LOG(parent->keys[i]);

}
KVLeafNodeFin* BplusTreeList::leafSearch(const char* key) {
    ++leafSearchCnt;
    KVNodeFin* node = root.get();
    if (node == nullptr) return nullptr;
    bool matched;
    int height = 0;
    std::string k(key);
    // while (!node->isLeaf) {
    //     ++height;
    //     ++logCmpCnt;
    //     auto inner = (KVInnerNodeFin*) node;
    //     const uint8_t keycount = inner->keyCount;
    //     // for (int i=0; i<keycount; ++i)
    //         // LOG(inner->keys[i]);
    //     tmr_log.start();
    //     auto lower = std::lower_bound(inner->keys.begin(), inner->keys.begin()+keycount, k);
    //     int dist = std::distance(inner->keys.begin(), lower);
    //     tmr_log.stop();
    //     node = inner->children[dist].get();
    // }
    while (!node->isLeaf) {
        ++height;
        ++logCmpCnt;
        matched = false;
        auto inner = (KVInnerNodeFin*) node;
        const uint8_t keycount = inner->keyCount;
        tmr_log.start();
        for (uint8_t idx = 0; idx<keycount; ++idx) {
            node = inner->children[idx].get();
            if (strcmp(key, inner->keys[idx].c_str()) <= 0) {
                matched = true;
                break;
            }
        }
        tmr_log.stop();
        if (!matched) node = inner->children[keycount].get();
    }
    if (depth < height)
        depth = height;
    return (KVLeafNodeFin*) node;
}
// now we have two nodes(inner node or leaf node), old one and new one.
// the new one is split from the old one
// so the new one must has a inner node to index it
void BplusTreeList::innerUpdateAfterSplit(KVNodeFin* node, std::unique_ptr<KVNodeFin> new_node, std::string* split_key) {
    // no inner node yet
    // ++innerUpdateCnt;
    if (!node->parent) {
        auto top = std::make_unique<KVInnerNodeFin>();
        top->keyCount = 1;
        top->keys[0] = *split_key;
        node->parent = top.get();
        new_node->parent = top.get();
        top->children[0] = std::move(root);
        top->children[1] = std::move(new_node);
        
        root = std::move(top);
        return;
    }
    KVInnerNodeFin* inner = node->parent;
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
    if (keycount <= INNER_KEYS_L)
        return;
    
    // this inner node is full either, split it, and update parents if necessary
    auto new_inner = std::make_unique<KVInnerNodeFin>();
    new_inner->parent = inner->parent;
    for (int i=INNER_KEYS_UPPER_L; i<keycount; ++i) {
        new_inner->keys[i-INNER_KEYS_UPPER_L] = std::move(inner->keys[i]);
    }
    for (int i=INNER_KEYS_UPPER_L; i<keycount+1; ++i) {
        new_inner->children[i-INNER_KEYS_UPPER_L] = std::move(inner->children[i]);
        new_inner->children[i-INNER_KEYS_UPPER_L]->parent = new_inner.get();
    }
    new_inner->keyCount = INNER_KEYS_MIDPOINT_L;
    std::string new_split_key = inner->keys[INNER_KEYS_MIDPOINT_L];
    inner->keyCount = INNER_KEYS_MIDPOINT_L;
    
    innerUpdateAfterSplit(inner, std::move(new_inner), &new_split_key);
}

int BplusTreeList::Scan(const kvObj* beginKey, int n, scanRes* r) {
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    if (n<1)
        return 0;
    auto leafnode = leafSearch(beginKey->data());
    while (leafnode) {      
        for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
            int x=strcmp(itor->key.c_str(), beginKey->data());
            if (x>= 0) {
                r->elems.push_back((*itor->ptr)->val());
                --n;
            } 
            if (n == 0)
                break;
        }
        if (n == 0)
            break;
        leafnode = leafnode->next;
        
    }
    r->done.Release_Store(reinterpret_cast<void*>(1));
    return 0;
}
int BplusTreeList::Scan(const kvObj* beginKey, const kvObj* lastKey, scanRes* r) {
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    if (beginKey == nullptr || lastKey == nullptr)
        return -1;
    auto leafnode = leafSearch(beginKey->data());
    uint8_t proc = 0;           
    while (leafnode) {
        for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
            if(proc==0 && strcmp(itor->key.c_str(), beginKey->data())< 0)
                continue; // search for start
            else {
                if (proc == 0)
                    proc = 1; //scaning
                if (proc == 1 && strcmp(itor->key.c_str(), lastKey->data()) > 0) {
                    proc = 2;
                    break; // search end
                }
                r->elems.push_back((*itor->ptr)->val());
            }
        }
        if (proc = 2)
            break;
        leafnode = leafnode->next;
        
    }
    r->done.Release_Store(reinterpret_cast<void*>(1));
    return 0;
}
int BplusTreeList::Scan(const std::string& beginKey, const std::string&lastKey, std::vector<std::string>& v) {
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    auto leafnode = leafSearch(beginKey.c_str());
    uint8_t proc = 0;           
    while (leafnode) {
        for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
            if(proc==0 && strcmp(itor->key.c_str(), beginKey.c_str())< 0)
                continue; // search for start
            else {
                if (proc == 0)
                    proc = 1; //scaning
                if (proc == 1 && strcmp(itor->key.c_str(), lastKey.c_str()) > 0) {
                    proc = 2;
                    break; // search end
                }
                v.push_back(std::string((*itor->ptr)->val()));

            }
        }
        if (proc = 2)
            break;
        leafnode = leafnode->next;
        
    }
    // output.done.Release_Store(reinterpret_cast<void*>(1));
    return 0;
}
int BplusTreeList::Scan(const std::string& beginKey, int n, std::vector<std::string>& v) {
    // LOG("Range: [ " << beginKey << ", " << lastKey <<" ]");
    if (n<1)
        return 0;
    auto leafnode = leafSearch(beginKey.c_str());
    while (leafnode) {      
        for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
            int x=strcmp(itor->key.c_str(), beginKey.c_str());
            if (x>= 0) {
                v.push_back(std::string((*itor->ptr)->val()));
                --n;
            } 
            if (n == 0)
                break;
        }
        if (n == 0)
            break;
        leafnode = leafnode->next;
        
    }
    // output.done.Release_Store(reinterpret_cast<void*>(1));
    return 0;
}

int BplusTreeList::Delete(const std::string& key) {
    auto leafnode = leafSearch(key.c_str());   
    if (leafnode) {
        const uint8_t hash = PearsonHash(key.c_str(), key.size());
        for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
            if (itor->hash == hash) {
                int x=strcmp(itor->key.c_str(), key.c_str());
                if (x==0) {
                    delete *(itor->ptr);
                    leafnode->leaf->lst.erase(itor->ptr);
                    leafnode->lst.erase(itor);
                    return 0;
                } else if (x > 0) {
                    return -1;
                }
            }
        }
    }
    return -1;
}
int BplusTreeList::Delete(const kvObj* key) {
auto leafnode = leafSearch(key->data());   
if (leafnode) {
    const uint8_t hash = PearsonHash(key->data(), key->size());
    for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
        if (itor->hash == hash) {
            int x=strcmp(itor->key.c_str(), key->data());
            if (x==0) {
                delete *(itor->ptr);
                leafnode->leaf->lst.erase(itor->ptr);
                leafnode->lst.erase(itor);
                return 0;
            } else if (x > 0) {
                return -1;
            }
        }
    }
}
return -1;
}
int BplusTreeList::Get(const std::string& key, std::string* val) {
//    LOG("Get Key " << key.c_str());
    auto leafnode = leafSearch(key.c_str());
    if (leafnode) {
        const uint8_t hash = PearsonHash(key.c_str(), key.size());
        for (auto itor=leafnode->lst.begin(); itor!=leafnode->lst.end(); ++itor) {
            if (itor->hash == hash) {
                ++leafCmpCnt;
                int x=itor->key.compare(key);
                if (x==0) {
                    val->assign((*itor->ptr)->val());
                    return 0;
                } else if (x > 0) {
                    ++notFoundCnt;
                    return -1;
                }
            }
        }
    }
    return -1;
}


// Pearson hashing lookup table from RFC 3074
const uint8_t PEARSON_LOOKUP_TABLE_L[256] = {
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
uint8_t BplusTreeList::PearsonHash(const char* data, const size_t size) {
    auto hash = (uint8_t) size;
    for (size_t i = size/2; i > 0;) {
        hash = PEARSON_LOOKUP_TABLE_L[hash ^ data[--i]];
    }
    // MODIFICATION START
    return (hash == 0) ? (uint8_t) 1 : hash;                             // 0 reserved for "null"
    // MODIFICATION END
}
    
}
