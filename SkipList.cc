#include "SkipList.h"
// #include "Timer.h"
namespace hybridKV {

SkipList::SkipList(int idx) : max_height(1), head_(newNode(NULL, NULL, false)), cmpCnt(0), maxCmpCnt(0),
	rnd_(0xdeadbeef), bgthread_started(false), idx_(idx) {
	for (int i=0; i<limit_height; ++i) {
		head_->setNext(i, NULL, true);
	}
    cmdQue.clear();
//    pthread_create();
}



bool SkipList::keyIsAfter(void *tarKey, Node *now) {
    if (now == nullptr)
        return false;
// #ifdef PM_READ_LATENCY_TEST
//     pload((uint64_t*)(now->key_), sizeof(kvObj));
// #endif
    // Strictly, we must compare tarKey with now->cached_key
    // But, for performance testing, the only difference 
    // between now->key_ and now->cached_key is add latency or not.
    // So, why not use func Compare(); for simplicity. 
    return Compare(*(kvObj *)(now->key_), *(kvObj *)(tarKey)) < 0;
}

SkipList::Node* SkipList::findCeilNode(void *key, Node **prev) {
    Node *res = nullptr;
    //code
    Node *itor = head_;
    int level = max_height-1;
    int tmpCmp = 0;
    while (true) {
        Node *next = itor->Next(level);
        
// #ifdef PM_READ_LATENCY_TEST
// //        emulate_latency_ns(PM_LATENCY_READ); // load A pointer of Node*
//         pload(itor->levelAddr(level), sizeof(Node*));
// #endif  
        ++tmpCmp;
        if (keyIsAfter(key, next))
            itor = next;
        else {
            if (prev != nullptr) prev[level] = itor;
            if (level == 0) {
                if (maxCmpCnt < tmpCmp)
                    maxCmpCnt = tmpCmp;
                cmpCnt += tmpCmp;
                return next;
            }
            else
                --level;
        }
    }
    return res;
}

// shall not be used.
int SkipList::GetVal(const std::string& keyStr, std::string *value) {
    kvObj* key = new kvObj(keyStr, false);
    Node *inode = findCeilNode((void *)key, nullptr);
    if (equal(*key, *(kvObj *)inode->key_)) {

        value->assign(((kvObj *)inode->val_)->data(), ((kvObj *)inode->val_)->size());
        return 0;
    }
    return -1;
}

int SkipList::Insert(Node* node) {
//    SkipList::Node* node = reinterpret_cast<Node*>(cmdNode->priv);
    if (!node->Valid())
        return -1;
    // int nhigh = randomHeight();
    Node *prev[limit_height];
    Node *inode = findCeilNode(node->key_, prev);
//    assert(inode == nullptr || *(kvObj*)(inode->key_) != *(kvObj*)(node->key_));

    
    // an real insert
    auto nhigh = node->height;
    if (nhigh > max_height) {
        for (int i=max_height; i<nhigh; ++i)
            prev[i] = head_;
        
        max_height = nhigh;
    }
    // inode = newNode(key, val, nhigh);
    // inode = (Node *)node;
    for (int i=0; i<nhigh; ++i) {
        node->setNext(i, prev[i]->Next(i), true);
//        pflush((uint64_t*), <#size_t len#>)
        prev[i]->setNext(i, node, false);
    }
    
    return 0;
}


int SkipList::Delete(const std::string& keyStr) {
//    mtx_.Lock();
//    LOG("SkipList::Delete()");
//    mtx_.unLock();
    kvObj* key= new kvObj(keyStr, false);
    Node *prev[limit_height];
    Node *delNode = findCeilNode((void*)key, prev);
    if (delNode == nullptr) {
        LOG("no node to delete in skiplist");
        return -1;
    }
    // find the equal key, fill prev[] to record previous pointer
    //assert(equal(*(static_cast<kvObj *>(delNode->key_)), *(static_cast<kvObj *>(key))));
    
    int hgt = delNode->height;
    for (int i=0; i<hgt; ++i) {
        prev[i]->forward[i] = delNode->forward[i];
    }

    // markdown: Node space and appendix (Node *) need to be free, don't know is this a right way
    // update answear: yes, it is
    delete [] (char *)delNode;
    while (max_height>1 && head_->forward[max_height] == nullptr)
        --max_height;
    return 0;
}
// Since all point operation first go through Hash Table, the target Node address can be got from HT
int SkipList::Update(const std::string& keyStr, const std::string& valStr) {

    kvObj* key = new kvObj(keyStr, false);
    kvObj* val = new kvObj(valStr, true);
    Node *node = findCeilNode((void*)key, nullptr);
    if (node == nullptr)
        return -1;
    //assert(equal(*(reinterpret_cast<kvObj*>(key)), *(reinterpret_cast<kvObj*>(tar->key_))));
    node->val_ = val;
    return 0;
}

SkipList::Node* SkipList::newNode(void *key, void *val, bool normal) {
    int height = normal? this->randomHeight() : limit_height;
    char *mem = new char [sizeof(Node) + sizeof(Node *) * (height -1)];
    return new(mem) Node(key, val, height);
    
}
int SkipList::Scan(const std::string& skey, int n, std::vector<std::string>& val) {

    kvObj* startkey = new kvObj(skey, false);
    if (n > 0 && startkey != nullptr) {
        Node *itor = findCeilNode(startkey, nullptr);
        uint64_t cnt = n-1;
        while (itor!=nullptr && cnt>0) {
            val.push_back(((kvObj*)itor->val_)->data());
            itor = itor->Next(0);
            --cnt;
        }
        // res.done.Release_Store(reinterpret_cast<void*>(1));
        return (int)cnt;
    }
    return -1;
}
int SkipList::Scan(void *startkey, void* limit, scanRes &res) {
    if (startkey != nullptr && limit != nullptr) {
        Node *itor = findCeilNode(startkey, nullptr);
        while (itor!=nullptr && *(kvObj*)(itor->key_) <= *(kvObj*)(limit)) {
            res.elems.push_back(((kvObj*)itor->val_)->data());
            itor = itor->Next(0);
        }
        res.done.Release_Store(reinterpret_cast<void*>(1));
        return 0;
    }
    return -1;
}
void SkipList::scanAll() {
    Node* itor = head_->Next(0);
    while (itor != nullptr) {
        std::cout <<"key: " << std::string(((kvObj*)(itor->key_))->data());
        std::cout <<"val: " << std::string(((kvObj*)(itor->val_))->data()) <<std::endl;
        itor = itor->Next(0);
    }
}


}