#include <iostream>
#include <map>
#include <unordered_map>
#include <cstdio>
#include "Configure.h"
// //#include "Db.h"
#include "PmemKV.h"
#include "HiKV.h"

#include "HashTable.h"
#include "BplusTree.h"
#include "BplusTreeList.h"
#include "SkipList.h"
#include "kvObject.h"
#include "microBench.h"
#include "hyKV_def.h"
// #include "Tool.h"
//#include "BplusTree.h"
using namespace hybridKV;

const int flag_write_count = 4 * 1000 * 1000;

const int flag_read_count = 1 * 1000 * 1000;

const int flag_sl_size = 10;
const int mbi = 1000 * 1000;
//const int
int threadnums = 10;
int DBid = 2;
int cpu_speed_mhz = 3100;
int pm_latecny_write = 400;
int pm_latency_read = 100;

int kv_nums = 50000000;

int keySizeIn(int x, int y) {
    return 64;
}

bool isBreakPoint(int i) {
    return (i==5*mbi) || (i==10*mbi) || (i==20*mbi) || (i==30*mbi) || (i==40*mbi) || (i==50*mbi) 
            || (i==60*mbi) || (i==70*mbi) || (i==80*mbi) || (i==90*mbi) || (i==100*mbi);
}

void testBplusTreeList(int num, int keyIdx) {
    FILE* logFd = fopen("/home/why/logBTree116.txt","a+");
    fprintf(logFd,"Function testBplusTreeList performance.(List version).\n");
    printf("Function testBplusTreeList performance.(List version).\n");
    #ifdef PM_WRITE_LATENCY_TEST
    LOG("PM WR latency = "<< pm_latecny_write);
#endif
    
#ifdef PM_READ_LATENCY_TEST
    LOG("PM RD latency = "<< pm_latency_read);
#endif
    Config* cfg = new Config();
    auto tree = new BplusTreeList(0);
    std::vector<std::string> keys;
    std::vector<int> keyLens = {16, 40, 64};
    int keySize;
    int nums = num * mbi;
    fprintf(logFd, "Loading data, size = %dM.", nums/mbi);
    if (keyIdx < 3)
        fprintf(logFd, "key Length = %dB\n", keyLens[keyIdx]);
    else
        fprintf(logFd, "key Length is uniform of {16, 40, 64}\n");
    LOG("Loading data, size = " << nums/mbi << "M.");
    uint32_t valLength = 100;

    TimerRDT tmr_obj, tmr_load, tmr_idle;
    for (int i=1; i<nums+1; ++i) {
        if (keyIdx == 3)
            keySize = keyLens[random() % 3];
        else 
            keySize = keyLens[keyIdx]; 
        auto key = randomString(keySize);
        auto val = randomString(valLength);
        keys.push_back(key);

        tmr_obj.start();
        auto k_obj = new kvObj(key, true);
        auto v_obj = new kvObj(val, true);
        auto kv_pair = new KVPairFin(k_obj, v_obj);
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)k_obj->data(), k_obj->size());
        pflush((uint64_t*)v_obj->data(), v_obj->size());
        pflush((uint64_t*)kv_pair, sizeof(KVPairFin));
#endif
        tmr_obj.stop();

        tmr_load.start();
        tree->Insert(kv_pair);
        tmr_load.stop();

        tmr_idle.start();
        tmr_idle.stop();

        // period time statistic
        if (isBreakPoint(i)) {
            fprintf(logFd, "Loading %dM...\n ", i/mbi);
            fprintf(logFd, "Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            fprintf(logFd, "Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());

            LOG("--------------------statistics-----------------------");
            printf("Loading %dM...\n ", i/mbi);
            printf("Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            printf("Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());
            printf("search time = %d(us).\n", tree->tmr_search.getDuration());
            printf("insert time  = %d(us).\n", tree->tmr_insert.getDuration());
            printf("leaf split time  = %d(us).\n", tree->tmr_split.getDuration());
            printf("inner update time  = %d(us).\n", tree->tmr_inner.getDuration());
            printf("skewed Leaf count = %d.\n", tree->skewLeafCnt);
            TimerRDT tmr_rd, tmr_idle_rd;
            int sId = random() % (keys.size()-2*mbi-1);
            for (int k=0; k<2*mbi; ++k) {
                std::string rdKey = keys[sId+k];
                std::string rdVal;

                tmr_rd.start();
                tree->Get(rdKey, &rdVal);
                tmr_rd.stop();

                tmr_idle_rd.start();
                tmr_idle_rd.stop();
            }
            fprintf(logFd, "Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());
            printf("Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());
        }

    }
    LOG("leafSeach time " << tree->leafSearchCnt);
    LOG("depth of the Tree " << tree->depth);
    LOG("leafnode count " << tree->leafCnt);
    LOG("innernode split time " << tree->innerUpdateCnt);
    LOG("leafnode split time " << tree->leafSplitCnt << ". Compare count " << tree->leafSplitCmp);
    LOG("leaf set slot compare count " << tree->leafCmpCnt);
    LOG("dup key cnt = " << tree->dupKeyCnt);
    LOG("BplusTree test done.");

    fprintf(logFd, "Loading %dM KV pair finished.\n", nums/mbi);
    printf("Loading %dM KV pair finished.\n", nums/mbi);

    while(true) {
        std::cout << "Choose Options: 1-Insert x(M), 2-Get x(M), 3-Delete x(M), 4-Update x(M), 5-Scan x(M), 6-Quit" <<std::endl;
        int op, sz; 
        std::cin >> op >> sz;
        if (op == 1) {
            TimerRDT tmr_alloc, tmr_op, tmr_idle_op;
            for (int k=0; k<sz*mbi; ++k) {
                if (keyIdx == 3)
                    keySize = keyLens[random() % 3];
                else 
                    keySize = keyLens[keyIdx]; 
                auto ky = randomString(keySize);
                auto vl = randomString(valLength);
                keys.push_back(ky);
                
                tmr_alloc.start();
                auto k_obj_wr = new kvObj(ky, true);
                auto v_obj_wr = new kvObj(vl, true);
                auto kv_pair = new KVPairFin(k_obj_wr, v_obj_wr);
#ifdef PM_WRITE_LATENCY_TEST
                pflush((uint64_t*)k_obj_wr->data(), k_obj_wr->size());
                pflush((uint64_t*)v_obj_wr->data(), v_obj_wr->size());
                pflush((uint64_t*)kv_pair, sizeof(KVPairFin));
#endif
                tmr_alloc.stop();

                tmr_op.start();
                tree->Insert(kv_pair);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            
            fprintf(logFd, "Under %dM KV pairs. Malloc/Insert %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration());   
            printf("Under %dM KV pairs. Malloc/Insert %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration());         
            printf("skewed Leaf count = %d.\n", tree->skewLeafCnt);
            nums+=sz*mbi;
            printf("DB Size = %dM\n", nums / mbi);
        } else if (op == 2) {
            TimerRDT tmr_op, tmr_idle_op;
            assert(nums == keys.size());
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];
                std::string rdVal;

                tmr_op.start();
                tree->Get(key_r, &rdVal);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 3) {
            std::cout << "Deletion needs to be implemented." <<std::endl;
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];

                tmr_op.start();
                tree->Delete(key_r);
                tmr_op.stop();
                
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());
            nums -= sz*mbi;
            keys.erase(keys.begin()+sId, keys.begin()+sId+sz*mbi);
            printf("DB Size = %dM\n", nums / mbi);
        } else if (op == 4) {
            TimerRDT tmr_alloc, tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = keys[sId+k];
                auto vl = randomString(valLength);

                tmr_alloc.start();
                auto k_obj_wr = new kvObj(ky, false);
                auto v_obj_wr = new kvObj(vl, true);
                auto kv_pair = new KVPairFin(k_obj_wr, v_obj_wr);
#ifdef PM_WRITE_LATENCY_TEST
                pflush((uint64_t*)k_obj_wr->data(), k_obj_wr->size());
                pflush((uint64_t*)v_obj_wr->data(), v_obj_wr->size());
                pflush((uint64_t*)kv_pair, sizeof(KVPairFin));
#endif
                tmr_alloc.stop();

                tmr_op.start();
                tree->Insert(kv_pair);
                tmr_op.stop();

                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Malloc/Update %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Malloc/Update %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("skewed Leaf count = %d.\n", tree->skewLeafCnt);
        } else if (op == 5) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = keys[sId+k];
                // auto vl = randomString(valLength);
                std::vector<std::string> val;

                tmr_op.start();
                tree->Scan(ky, 10, val);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Scan %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Scan %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        
        } else if (op == 6) {
            break;
        } else {
            std::cout << "wrong option" <<std::endl;
        }
    }
    fclose(logFd);
}

void testHashTable(int num, int keyIdx) {
    FILE* logFd = fopen("/home/why/logHT116.txt","a+");
    LOG("Function testHashTable performance.");
    fprintf(logFd,"Function testHashTable performance.\n");
#ifdef PM_WRITE_LATENCY_TEST
    fprintf(logFd, "PM Write Latency = %d ns\n", pm_latecny_write);
    LOG("PM WR latency = "<< pm_latecny_write);
#endif
    
#ifdef PM_READ_LATENCY_TEST
    fprintf(logFd, "PM Read latency = %d ns\n", pm_latency_read);
    LOG("PM RD latency = "<< pm_latency_read);
#endif
    Config* cfg = new Config();
    auto ht = new HashTable(cfg->ht_size, cfg->ht_limits, cfg->hasher, true);
    std::vector<std::string> keys;
    std::vector<int> keyLens = {16, 40, 64};
    int keySize;
    int nums = num * mbi;
    fprintf(logFd, "Loading data, size = %dM.", nums/mbi);
    if (keyIdx < 4)
        fprintf(logFd, "key Length = %dB\n", keyLens[keyIdx]);
    else
        fprintf(logFd, "key Length is uniform of {16, 40, 64}\n");
    LOG("Loading data, size = " << nums/mbi << "M.");
    uint32_t valLength = 100;

    TimerRDT tmr_obj, tmr_load, tmr_idle;
    for (int i=1; i<nums+1; ++i) {
        if (keyIdx == 3)
            keySize = keyLens[random() % 3];
        else 
            keySize = keyLens[keyIdx]; 
        auto key = randomString(keySize);
        auto val = randomString(valLength);
        keys.push_back(key);

        // time of "new OBJ"
        tmr_obj.start();
        auto k_obj = new kvObj(key, true);
        auto v_obj = new kvObj(val, true);
#ifdef PM_WRITE_LATENCY_TEST
        pflush((uint64_t*)k_obj->data(), k_obj->size());
        pflush((uint64_t*)v_obj->data(), v_obj->size());
#endif
        tmr_obj.stop();

        // time of "Set"
        tmr_load.start();
        Dict::dictEntry* entry = nullptr;
        ht->Set(k_obj, v_obj, &entry);
        tmr_load.stop();

        // time of "timer Looping"
        tmr_idle.start();
        tmr_idle.stop();

        // period time statistic

        if (isBreakPoint(i)) {
            fprintf(logFd, "Loading %dM...\n ", i/mbi);
            fprintf(logFd, "Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            fprintf(logFd, "Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());
            fprintf(logFd, "conflictCnt = %d, Max conflict = %d, dupKeyCnt = %d.\n",ht->conflictCnt(), ht->maxConflict(), ht->dupKeyCnt());
            printf("Loading %dM...\n ", i/mbi);
            printf("Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            printf("Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());
            TimerRDT tmr_rd, tmr_idle_rd;
            int sId = random() % (keys.size()-2*mbi-1);
            for (int k=0; k<2*mbi; ++k) {
                std::string rdKey = keys[sId+k];
                std::string rdVal;

                tmr_rd.start();
                ht->Get(rdKey, &rdVal);
                tmr_rd.stop();

                tmr_idle_rd.start();
                tmr_idle_rd.stop();
            }
            fprintf(logFd, "Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());
            printf("Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());

        }

    }
    fprintf(logFd, "Loading %dM KV pair finished.\n", nums/mbi);
    printf("Loading %dM KV pair finished.\n", nums/mbi);
    while(true) {
        std::cout << "Choose Options: 1-Insert x(M), 2-Get x(M), 3-Delete x(M), 4-Update x(M), 5-Quit" <<std::endl;
        int op, sz; 
        std::cin >> op >> sz;
        if (op == 1) {
            TimerRDT tmr_alloc, tmr_op, tmr_idle_op;
            for (int k=0; k<sz*mbi; ++k) {
                if (keyIdx == 3)
                    keySize = keyLens[random() % 3];
                else 
                    keySize = keyLens[keyIdx]; 
                auto ky = randomString(keySize);
                auto vl = randomString(valLength);
                keys.push_back(ky);
                
                tmr_alloc.start();
                auto k_obj_wr = new kvObj(ky, true);
                auto v_obj_wr = new kvObj(vl, true);
                // emulate_latency_ns(400);
                // // emulate_latency_ns(1200);
#ifdef PM_WRITE_LATENCY_TEST
                pflush((uint64_t*)k_obj_wr->data(), k_obj_wr->size());
                pflush((uint64_t*)v_obj_wr->data(), v_obj_wr->size());
#endif
                tmr_alloc.stop();
                tmr_op.start();
                Dict::dictEntry* entry = nullptr;
                ht->Set(k_obj_wr, v_obj_wr, &entry);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            
            fprintf(logFd, "Under %dM KV pairs. Malloc/Insert %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration());   
            printf("Under %dM KV pairs. Malloc/Insert %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration());         
            nums+=sz*mbi;
            printf("DB Size = %dM\n", nums / mbi);
        } else if (op == 2) {
            TimerRDT tmr_op, tmr_idle_op;
            assert(nums == keys.size());
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];
                std::string rdVal;

                tmr_op.start();
                ht->Get(key_r, &rdVal);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 3) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];
            
                tmr_op.start();
                ht->Delete(key_r);
                tmr_op.stop();
                
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());
            nums -= sz*mbi;
            keys.erase(keys.begin()+sId, keys.begin()+sId+sz*mbi);
            printf("DB Size = %dM", nums / mbi);
        } else if (op == 4) {
            TimerRDT tmr_alloc, tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                // LOG("U k=" << k);
                auto ky = keys[sId+k];
                auto vl = randomString(valLength);

                tmr_alloc.start();
                auto k_obj_wr = new kvObj(ky, false);
                auto v_obj_wr = new kvObj(vl, true);
                pflush((uint64_t*)v_obj_wr->data(), v_obj_wr->size());
                // emulate_latency_ns(1000);
                tmr_alloc.stop();
                tmr_op.start();
                Dict::dictEntry* entry = nullptr;
                ht->Set(k_obj_wr, v_obj_wr, &entry);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Malloc/Update %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Malloc/Update %dM takes %d/%d (us).\n", nums/mbi, sz, tmr_alloc.getDuration()-tmr_idle_op.getDuration(), tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 5) {
            break;
        }else {
            std::cout << "wrong option" <<std::endl;
        }
    }
    fclose(logFd);
}

void testSkipList(int num, int keyIdx) {
    FILE* logFd = fopen("/home/why/logSL116.txt","a+");
    LOG("Function testSkipList performance.");
    fprintf(logFd, "Function testSkipList performance.\n");
#ifdef PM_WRITE_LATENCY_TEST
    fprintf(logFd, "PM Write Latency = %d ns\n", pm_latecny_write);
    LOG("PM WR latency = "<< pm_latecny_write);
#endif
    
#ifdef PM_READ_LATENCY_TEST
    fprintf(logFd, "PM Read latency = %d ns\n", pm_latency_read);
    LOG("PM RD latency = "<< pm_latency_read);
#endif
    Config* cfg = new Config();
    auto sl = new SkipList(0);
    std::vector<std::string> keys;
    std::vector<int> keyLens = {16, 40, 64};
    int keySize;
    int nums = num * mbi;
    uint32_t valLength = 100;

    fprintf(logFd, "Loading data, size = %dM.", nums/mbi);
    if (keyIdx < 4)
        fprintf(logFd, "key Length = %dB\n", keyLens[keyIdx]);
    else
        fprintf(logFd, "key Length is uniform of {16, 40, 64}\n");
    LOG("Loading data, size = " << nums/mbi << "M.");


    TimerRDT tmr_obj, tmr_load, tmr_idle;
    for (int i=1; i<nums+1; ++i) {
        if (keyIdx == keyLens.size())
            keySize = keyLens[random() % keyLens.size()];
        else 
            keySize = keyLens[keyIdx]; 
        auto key = randomString(keySize);
        auto val = randomString(valLength);
        keys.push_back(key);

        // time of "new OBJ"
        tmr_obj.start();
        auto k_obj = new kvObj(key, true);
        auto v_obj = new kvObj(val, true);
        tmr_obj.stop();

        // time of "Set"
        tmr_load.start();
        SkipList::Node* slNode = sl->newNode((void*)k_obj, (void*)v_obj, true);
        sl->Insert(slNode);
        tmr_load.stop();

        // time of "timer Looping"
        tmr_idle.start();
        tmr_idle.stop();

        // period time statistic
        if (isBreakPoint(i)) {
            fprintf(logFd, "Loading %dM...\n ", i/mbi);
            fprintf(logFd, "Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            fprintf(logFd, "Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());
            
            printf("Loading %dM...\n ", i/mbi);
            printf("Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            printf("Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());
            TimerRDT tmr_rd, tmr_idle_rd;
            int sId = random() % (keys.size()-2*mbi-1);
            for (int k=0; k<2*mbi; ++k) {
                std::string rdKey = keys[sId+k];
                std::string rdVal;

                tmr_rd.start();
                sl->GetVal(rdKey, &rdVal);
                tmr_rd.stop();

                tmr_idle_rd.start();
                tmr_idle_rd.stop();
            }
            fprintf(logFd, "Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());
            printf("Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());

        }
    }
    fprintf(logFd, "Loading %dM KV pair finished.\n", nums/mbi);
    printf("Loading %dM KV pair finished.\n", nums/mbi);
    while(true) {
        std::cout << "Choose Options: 1-Insert x(M), 2-Get x(M), 3-Delete x(M), 4-Update x(M), 5-Scan, 6-Quit" <<std::endl;
        int op, sz; 
        std::cin >> op >> sz;
        if (op == 1) {
            TimerRDT tmr_op, tmr_idle_op;
            for (int k=0; k<sz*mbi; ++k) {
                if (keyIdx == keyLens.size())
                    keySize = keyLens[random() % keyLens.size()];
                else 
                    keySize = keyLens[keyIdx]; 
                auto ky = randomString(keySize);
                auto vl = randomString(valLength);
                keys.push_back(ky);
                tmr_op.start();
                auto k_obj_wr = new kvObj(ky, true);
                auto v_obj_wr = new kvObj(vl, true);
                SkipList::Node* slNodeX = sl->newNode((void*)k_obj_wr, (void*)v_obj_wr, true);
                sl->Insert(slNodeX);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            
            fprintf(logFd, "Under %dM KV pairs. Insert %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());   
            printf("Under %dM KV pairs. Insert %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());         
            nums+=sz*mbi;
            printf("DB Size = %dM", nums / mbi);
        } else if (op == 2) {
            TimerRDT tmr_op, tmr_idle_op;
            assert(nums == keys.size());
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];
                std::string rdVal;

                tmr_op.start();
                sl->GetVal(key_r, &rdVal);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 3) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];

                tmr_op.start();
                sl->Delete(key_r);
                tmr_op.stop();
                
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());
            nums -= sz*mbi;
            keys.erase(keys.begin()+sId, keys.begin()+sId+sz*mbi);
            printf("DB Size = %dM", nums / mbi);
        } else if (op == 4) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = keys[sId+k];
                auto vl = randomString(valLength);

                tmr_op.start();
                sl->Update(ky, vl);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Update %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Update %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 5) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = keys[sId+k];
                // auto vl = randomString(valLength);
                std::vector<std::string> val;

                tmr_op.start();
                sl->Scan(ky, 10, val);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Scan %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Scan %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        
        } else if (op == 6) {
            break;
        } else {
            std::cout << "wrong option" <<std::endl;
        }
    }
    fclose(logFd);
}
void testUniformWrite() {

}

void testGevWrite() {
    
}

void dataStructureTest() {
    int ch;
    while (true) {
        std::cout << "Choose A DS : 0 - HashTable,  1 - B+Tree,  2 - SkipList, 3 - Quit" <<std::endl;
        std::cin >> ch;
        if(ch == 3)
            return;
        std::cout << "Set Dataset Size(M)(default 50M): ";
        int size=0, keyLen;
        std::cin >> size;
        std::cout << "Set Key Length: {0-16, 1-32, 2-48, 3-64, 4-uniform, 5-gev}: ";
        std::cin >> keyLen;
        if (ch == 0) {
            testHashTable(size, keyLen);
        } else if (ch == 1) {
            testBplusTreeList(size, keyLen);
        } else if (ch==2) {
            testSkipList(size, keyLen);
        } else {
            std::cout<< "Are you fucking blind?" <<std::endl;
        }
        std::cout << "Continue?(y/n)" <<std::endl;
        char x;
        std::cin >> x;
        if (x == 'n') {
            std::cout << "exit dataStructureTest()." <<std::endl;
            break;
        }
    }
}


void ycsbTest() {

}
std::string day = "118";
std::string fname = "/home/why/log";
void microBench(int num, int keyIdx, const std::string& name) {
    // std::string fname = "/home/why/log"
    FILE* logFd = fopen((fname+name+day).c_str(),"a+");
    LOG("Function for "<< name << " performance test.");
    fprintf(logFd,"Function for %s performance test.\n", name.c_str());
#ifdef PM_WRITE_LATENCY_TEST
    fprintf(logFd, "PM Write Latency = %d ns\n", pm_latecny_write);
    LOG("PM WR latency = "<< pm_latecny_write);
#endif
    
#ifdef PM_READ_LATENCY_TEST
    fprintf(logFd, "PM Read latency = %d ns\n", pm_latency_read);
    LOG("PM RD latency = "<< pm_latency_read);
#endif
    hyDB* db=nullptr;
    int bgNums = 0;
    if (name == "myKV") {
        std::cout << "Input the BG Btrees you want to create: ";
        std::cin >> bgNums;
        assert(bgNums > 0 && bgNums < 10);
    }

    hyDB::Open(&db, name, bgNums);

    if (db == nullptr)
        LOG("Failed to open db: "<< name);
    std::vector<std::string> keys;
    std::vector<int> keyLens = {16, 40, 64};
    int keySize;
    int nums = num * mbi;
    fprintf(logFd, "Loading data, size = %dM.", nums/mbi);
    if (keyIdx < 4)
        fprintf(logFd, "key Length = %dB\n", keyLens[keyIdx]);
    else
        fprintf(logFd, "key Length is uniform of {16, 40, 64}\n");
    LOG("Loading data, size = " << nums/mbi << "M.");
    uint32_t valLength = 100;

    TimerRDT tmr_load, tmr_idle;
    TimerRDT tmr_all;
    db->newRound();
    for (int i=1; i<nums+1; ++i) {
        if (keyIdx == 3)
            keySize = keyLens[random() % 3];
        else 
            keySize = keyLens[keyIdx]; 
        auto key = randomString(keySize);
        auto val = randomString(valLength);
        keys.push_back(key);

        tmr_load.start();
        db->Put(key, val);
        tmr_load.stop();

        tmr_idle.start();
        tmr_idle.stop();

        if(isBreakPoint(i)) {
            fprintf(logFd, "Loading %dM...\n ", i/mbi);
            // fprintf(logFd, "Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            fprintf(logFd, "Inert time = %d(us).\n", tmr_load.getDuration()-tmr_idle.getDuration());
            // fprintf(logFd, "conflictCnt = %d, Max conflict = %d, dupKeyCnt = %d.\n",ht->conflictCnt(), ht->maxConflict(), ht->dupKeyCnt());
            printf("Loading %dM...\n ", i/mbi);
            // if (name == "HiKV")
            //     printf("Push Queue Cost time: %d(us).", db->time() - tmr_idle.getDuration());
            // printf("Malloc time = %d(us).\n ", tmr_obj.getDuration()-tmr_idle.getDuration());
            printf("Inert time = %d(us).\n", tmr_load.getDuration()- tmr_idle.getDuration());
            TimerRDT tmr_rd, tmr_idle_rd;
            int sId = random() % (keys.size()-2*mbi-1);
            for (int k=0; k<2*mbi; ++k) {
                std::string rdKey = keys[sId+k];
                std::string rdVal;

                tmr_rd.start();
                db->Get(rdKey, &rdVal);
                tmr_rd.stop();

                tmr_idle_rd.start();
                tmr_idle_rd.stop();
            }
            fprintf(logFd, "Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());
            printf("Get perf test 2M, time = %d.\n", tmr_rd.getDuration()-tmr_idle_rd.getDuration());
            db->newRound();
        }


    }

    fprintf(logFd, "Loading %dM KV pair finished.\n", nums/mbi);
    printf("Loading %dM KV pair finished.\n", nums/mbi);

    while (true) {
        std::cout << "Choose Options: 1-Insert x(M), 2-Get x(M), 3-Delete x(M), 4-Update x(M), 5-scan, 6-Quit" <<std::endl;
        int op, sz; 
        std::cin >> op >> sz;
        if (op == 1) {
            TimerRDT tmr_op, tmr_idle_op;
            for (int k=0; k<sz*mbi; ++k) {
                if (keyIdx == 3)
                    keySize = keyLens[random() % 3];
                else 
                    keySize = keyLens[keyIdx]; 
                auto ky = randomString(keySize);
                auto vl = randomString(valLength);
                keys.push_back(ky);
                tmr_op.start();
                db->Put(ky, vl);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            
            fprintf(logFd, "Under %dM KV pairs. Insert %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());   
            printf("Under %dM KV pairs. Insert %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());         
            nums+=sz*mbi;
            printf("DB Size = %dM\n", nums / mbi);
        } else if (op == 2) {
            TimerRDT tmr_op, tmr_idle_op;
            assert(nums == keys.size());
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];
                std::string rdVal;

                tmr_op.start();
                db->Get(key_r, &rdVal);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs. Get %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 3) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto key_r = keys[sId+k];
            
                tmr_op.start();
                db->Delete(key_r);
                tmr_op.stop();
                
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Delete %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration());
            nums -= sz*mbi;
            keys.erase(keys.begin()+sId, keys.begin()+sId+sz*mbi);
            printf("DB Size = %dM", nums / mbi);
        } else if (op == 4) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = keys[sId+k];
                auto vl = randomString(valLength);

                tmr_op.start();
                db->Put(ky, vl);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Update %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Update %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 5) {
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto bkey = keys[sId+k];
                auto ekey = bkey;
                ekey[ekey.size()-1] += 10;
                // auto vl = randomString(valLength);
                std::vector<std::string> val;

                tmr_op.start();
                db->Scan(bkey, ekey, val);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Scan %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Scan %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 

        } else if (op == 6) {
            break;
        } else {
            std::cout << "wrong option" <<std::endl;
        }
    }
    fclose(logFd);
}
void testPmemKV() {

}
void testMyKV() {

}
void kvStoreTest() {
    int ch;
    while (true) {
        std::cout << "Choose A DS : 0 - myKV,  1 - HiKV,  2 - PmemKV, 3 - Quit" <<std::endl;
        std::cin >> ch;
        if (ch == 3)
            return;
        std::cout << "Set Dataset Size(M)(default 50M): ";
        int size=0, keyLen;
        std::cin >> size;
        std::cout << "Set Key Length: {0-16, 1-32, 2-48, 3-64, 4-uniform, 5-gev}: ";
        std::cin >> keyLen;
        if (ch == 0) {
            microBench(size, keyLen, "myKV");
        } else if (ch == 1) {
            microBench(size, keyLen, "HiKV");
        } else {
            microBench(size, keyLen, "PmemKV");
        }
        std::cout << "Continue?(y/n)" <<std::endl;
        char x;
        std::cin >> x;
        if (x == 'n') {
            std::cout << "exit kvStoreTest()." <<std::endl;
            break;
        }
    }
}
int main(int argc, char** argv) {

    long long n;
    char junk;
    // uint8_t ch;
    // std::cout << "Choose A DB : 0 - PmemKV  1 - HiKV  2 - hyKV" <<std::endl;
    // std::cin >>ch;
    for (int i=1; i<argc; ++i) {
        if (sscanf(argv[i], "-pnums=%lld%c", &n, &junk) == 1) {
            threadnums = (int)n;
        } else if (sscanf(argv[i], "-db=%lld%c", &n, &junk) == 1) {
            DBid = (int)n;
        } else if (sscanf(argv[i], "-pmWR=%lld%c", &n, &junk) == 1) {
            pm_latecny_write = (int)n;
        } else if (sscanf(argv[i], "-pmRD=%lld%c", &n, &junk) == 1) {
            pm_latency_read = (int)n;
        } else if (sscanf(argv[i], "-cpu=%lld%c", &n, &junk) == 1)  {
            cpu_speed_mhz = (int)n;
        } else if (sscanf(argv[i], "-knums=%lld%c", &n, &junk) == 1) {
            kv_nums = (int)n;
        }
        else {
            std::cout << "unknown arg" <<std::endl;
        }
    }
    init_pflush(cpu_speed_mhz, pm_latecny_write, pm_latency_read);
    dataStructureTest();
    // kvStoreTest();
    return 0;


//     hyDB* db = nullptr;
//     std::vector<std::string> kvid = {"PmemKV", "HiKV", "hyKV"}; 

//    // hyDB::Open(&db, pmemKV);
//     std::cout << "Testing DB " << kvid[DBid] <<std::endl;
//     std::cout << "BackGround threadnums = " << threadnums <<std::endl;  
//     hyDB::Open(&db, kvid[DBid], threadnums);
// //    db->showCfg();
//     std::cout << "DB Created: " <<kvid[DBid] <<std::endl;

}


void testBplusTreePair(int nums) {
    LOG("Function testBplusTree performance.(KVPair version)");
#ifdef PM_WRITE_LATENCY_TEST
    LOG("PM WR latency = "<< pm_latecny_write);
#endif
    
#ifdef PM_READ_LATENCY_TEST
    LOG("PM RD latency = "<< pm_latency_read);
#endif
    LOG("KV pair nums = " << nums / 1000000 << "M.");
    Config* cfg = new Config();
    auto tree = new BplusTree();
    std::vector<uint32_t> minLen(10);
    generateScope(kvUniform, minLen);
    uniformKeyGenerator gen(minLen);
    Random rndVal(301);
    uint32_t valLength = 100;
    TimerRDT tmr_opt, tmr_idle;
    for (int i=0; i<nums; ++i) {
        auto key = gen.nextStr();
        auto val = randomString(valLength);
        
        if (i==mbi || i==5*mbi || i==10 * mbi || i == 20*mbi || i == 30*mbi || i==40*mbi) {
            LOG("after " << i/mbi << "M insertion...");
            //tmr_obj.getDuration();
            tmr_opt.getDuration();
            tmr_idle.getDuration();

        }

        tmr_opt.start();
        tree->Put(key,val);
        tmr_opt.stop();

        tmr_idle.start();
        tmr_idle.stop();
    }
    LOG("after all (" << nums/mbi <<"M) insertion...");
    //tmr_obj.getDuration();
    tmr_opt.getDuration();
    tmr_idle.getDuration();
    LOG("leafSeach time " << tree->leafSearchCnt);
    LOG("depth of the Tree " << tree->depth);
    LOG("leafnode count " << tree->leafCnt);
    LOG("innernode split time " << tree->innerUpdateCnt);
    LOG("leafnode split time " << tree->leafSplitCnt << ". Compare count " << tree->leafSplitCmp);
    LOG("leaf set slot compare count " << tree->leafCmpCnt);
    LOG("BplusTree test done.");
}
// void testBplusTreeObj(int nums) {
//     FILE* logFd = fopen("/home/why/logFileBTree.txt","a+");
//     fprintf(logFd,"Function testBplusTree performance.(KVObj version).\n");
//     LOG("Function testBplusTree performance.(KVObj version).");
//     #ifdef PM_WRITE_LATENCY_TEST
//     LOG("PM WR latency = "<< pm_latecny_write);
// #endif
    
// #ifdef PM_READ_LATENCY_TEST
//     LOG("PM RD latency = "<< pm_latency_read);
// #endif
//     LOG("KV pair nums = " << nums / 1000000 << "M.");
//     Config* cfg = new Config();
//     auto tree = new BplusTree();
//     std::vector<uint32_t> minLen(10);
//     generateScope(kvUniform, minLen);
//     uniformKeyGenerator gen(minLen);
//     std::vector<std::string> keys;
//     std::vector<int> keyLens = {16, 32, 64, 96};
//     Random rndVal(301);
//     uint32_t valLength = 100;
//     TimerRDT tmr_obj, tmr_opt, tmr_idle;
//     for (int i=0; i<nums+1; ++i) {
//         int keySize = keyLens[0];
//         auto key = randomString(keySize);
//         auto val = randomString(valLength);
//         keys.push_back(key);
//         //LOG("round " << i);
//         if (i==5*mbi || i==10 * mbi || i == 20*mbi || i == 30*mbi || i==40*mbi || i == 50*mbi)  {
//             LOG("after " << i/mbi << "M insertion...");
//             fprintf(logFd, "after %dM insertion...\n", i/mbi);
//             fprintf(logFd, "Malloc time = %d(us).\n", tmr_obj.getDuration());
//             fprintf(logFd, "Insert time = %d(us).\n", tmr_opt.getDuration());
//             fprintf(logFd, "Idle time = %d(us).\n", tmr_idle.getDuration());
//             fprintf(logFd, "depth of the tree = %d.\n", tree->depth);
//             // tmr_obj.getDuration();
//             // tmr_opt.getDuration();
//             // tmr_idle.getDuration();
//             LOG("depth of the Tree " << tree->depth);
//             LOG("leafnode count " << tree->leafCnt);
//             LOG("innernode split time " << tree->innerUpdateCnt);
//             LOG("leafnode split time " << tree->leafSplitCnt << ". Compare count " << tree->leafSplitCmp);
//             LOG("leaf set slot compare count " << tree->leafCmpCnt);
//             LOG("dup key cnt = " << tree->dupKeyCnt);
//         }
//         if (i==5*mbi || i==10 * mbi || i == 20*mbi || i == 30*mbi || i==40*mbi || i == 50*mbi) {
//             TimerRDT tmr_rd, tmr_idle_rd;
//             const int rd_size = 10 * mbi;
//             for (int j=0; j<rd_size; ++j) {
//                 auto rdKey = keys[random() % keys.size()];
//                 std::string rdVal;

//                 tmr_rd.start();
//                 tree->Get(rdKey, &rdVal);
//                 tmr_rd.stop();
//                 tmr_idle_rd.start();
//                 tmr_idle_rd.stop();

//             }
//             LOG("under db size = " <<i/mbi << "M, get " << rd_size << " keys.");
//             fprintf(logFd, "under db size = %dM, get %dM keys.\n", i/mbi, rd_size/mbi);
//             fprintf(logFd, "Get time = %d(us).\n", tmr_rd.getDuration());
//             fprintf(logFd, "Idle time = %d(us).\n", tmr_idle_rd.getDuration());
   
//         }
//         tmr_obj.start();
//         auto k_obj = new kvObj(key, true);
//         auto v_obj = new kvObj(val, true);
//         tmr_obj.stop();

//         tmr_opt.start();
//         tree->Insert(k_obj->data(), v_obj->data());
//         tmr_opt.stop();

//         tmr_idle.start();
//         tmr_idle.stop();
//     }
//     LOG("after all (" << nums/mbi <<"M) insertion...");
//     fprintf(logFd, "after all (%dM) insertion...\n", nums/mbi);
//     fprintf(logFd, "Malloc time = %d(us).\n", tmr_obj.getDuration());
//     fprintf(logFd, "Insert time = %d(us).\n", tmr_opt.getDuration());
//     fprintf(logFd, "Idle time = %d(us).\n", tmr_idle.getDuration());      
//     TimerRDT tmr_wr;
//     for (int k=0; k<2*mbi; ++k) {
//         int keySz = keyLens[0];
//         auto wrKey = randomString(keySz);
//         auto wrVal = randomString(valLength);

//         tmr_wr.start();
//         auto k_obj_wr = new kvObj(wrKey, true);
//         auto v_obj_wr = new kvObj(wrVal, true);
//         tree->Insert(k_obj_wr->data(), v_obj_wr->data());
//         tmr_wr.stop();
//     }
//     TimerRDT tmr_r;
//     for (int k=0; k<2*mbi; ++k) {
//         auto rKey = keys[random() % keys.size()];
//         std::string rVal;

//         tmr_r.start();
//         tree->Get(rKey, &rVal);
//         tmr_r.stop();
//     }
//     fprintf(logFd, "When %dM, Read 2M time = %d(us).\n", nums/mbi, tmr_r.getDuration());
//     fprintf(logFd, "When %dM, Insert 2M time = %d(us).\n", nums/mbi, tmr_wr.getDuration());
    
//     LOG("leafSeach time " << tree->leafSearchCnt);
//     LOG("depth of the Tree " << tree->depth);
//     LOG("leafnode count " << tree->leafCnt);
//     LOG("innernode split time " << tree->innerUpdateCnt);
//     LOG("leafnode split time " << tree->leafSplitCnt << ". Compare count " << tree->leafSplitCmp);
//     LOG("leaf set slot compare count " << tree->leafCmpCnt);
//     LOG("dup key cnt = " << tree->dupKeyCnt);
//     LOG("BplusTree test done.");
// }