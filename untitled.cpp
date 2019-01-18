    FILE* logFd = fopen("/home/why/logBTree115.txt","a+");
    LOG("Function testBplusTree performance.");
    fprintf(logFd,"Function testBplusTree performance.\n");
#ifdef PM_WRITE_LATENCY_TEST
    fprintf(logFd, "PM Write Latency = %d ns\n", pm_latecny_write);
    LOG("PM WR latency = "<< pm_latecny_write);
#endif
    
#ifdef PM_READ_LATENCY_TEST
    fprintf(logFd, "PM Read latency = %d ns\n", pm_latency_read);
    LOG("PM RD latency = "<< pm_latency_read);
#endif
    Config* cfg = new Config();
    auto ht = new HashTable(cfg->ht_size, cfg->ht_seed, cfg->ht_limits, cfg->hasher, true);
    std::vector<std::string> keys;
    std::vector<int> keyLen = {16, 32, 48, 64};
    int keySize = keyLen[keyIdx];
    int nums = num * mbi;
    fprintf(logFd, "Loading data, size = %dM.", nums/mbi);
    if (keyIdx < 4)
        fprintf(logFd, "key Length = %dB\n", keyLen[keyIdx]);
    else
        fprintf(logFd, "key Length is uniform of {16, 32, 48, 64}\n");
    LOG("Loading data, size = " << nums/mbi << "M.");
    uint32_t valLength = 100;

    TimerRDT tmr_obj, tmr_load, tmr_idle;
    for (int i=1; i<nums+1; ++i) {
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
            TimerRDT tmr_op, tmr_idle_op;
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = randomString(keySize);
                auto vl = randomString(valLength);
                keys.push_back(ky);
                tmr_op.start();
                auto k_obj_wr = new kvObj(ky, true);
                auto v_obj_wr = new kvObj(vl, true);
                Dict::dictEntry* entry = nullptr;
                ht->Set(k_obj_wr, v_obj_wr, &entry);
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
            TimerRDT tmr_op, tmr_idle_op;
            int sId = random() % (nums-sz*mbi);
            for (int k=0; k<sz*mbi; ++k) {
                auto ky = keys[sId+k];
                auto vl = randomString(valLength);

                tmr_op.start();
                auto k_obj_wr = new kvObj(ky, false);
                auto v_obj_wr = new kvObj(vl, true);
                Dict::dictEntry* entry = nullptr;
                ht->Set(k_obj_wr, v_obj_wr, &entry);
                tmr_op.stop();
                tmr_idle_op.start();
                tmr_idle_op.stop();
            }
            fprintf(logFd, "Under %dM KV pairs, Update %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
            printf("Under %dM KV pairs, Update %dM takes %d (us).\n", nums/mbi, sz, tmr_op.getDuration()-tmr_idle_op.getDuration()); 
        } else if (op == 5) {
            break;
        }else {
            std::cout << "wrong option" <<std::endl;
        }
    }