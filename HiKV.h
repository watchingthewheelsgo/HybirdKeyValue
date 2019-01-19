//
//  BplusTree.cpp
//  devl
//
//  Created by 吴海源 on 2018/12/17.
//  Copyright © 2018 why. All rights reserved.
//
#ifndef __HIKV_H
#define __HIKV_H

#include <string>
#include <vector>
#include <deque>

#include "HashTable.h"
#include "kvObject.h"
#include "Configure.h"
#include "DBInterface.h"
#include "BplusTreeList.h"
#include "Thread.h"
#include "Tool.h"
// #include "Timer.h"
#include "hyKV_def.h"
#include "latency.hpp"

namespace hybridKV {
#ifdef HiKV_TEST
void* schedule2(void* arg);
	
struct cmdInfo {
	cmdType type;
	void* key;
	void* value;
	void* ptr;
};

class HiKV : public hyDB {
public:
	typedef std::deque::iterator QITOR;
	HiKV();
	~HiKV(){};
	
	int Get(const std::string& key, std::string* val);
	int Put(const std::string& key, const std::string& val);
	int Delete(const std::string& key);
	int Scan(const std::string& beginKey, int n, std::vector<std::string>& output) {
		return 0;
	}
	int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output);
	int Update(const std::string& key, const std::string& val);
	static void BGWork(void* db);
	void BgInit();
	
	int Recover();
	int Close();
	bool emptyQue() {
		return que.empty();
	}
	void queue_push(cmdInfo* cmd) {
		mt_.Lock();
		que.push_back(cmd);
		mt_.unLock();
	}
	cmdInfo* extraQue() {
		mt_.Lock();
		auto res = que.front();
		que.pop_front();
		mt_.unLock();
		return res;
	}
	BplusTreeList* tree() {
		return tree_;
	}
	// void debug() {
	//     tree_->showAll();
	// }
	uint64_t time() {
		return tmr.getDuration();
	}
	TimerRDT tmr;
private:
	uint32_t flushTh;
	Config* cfg;
	Mutex mt_;
	std::deque<cmdInfo*> que;
	BplusTreeList* tree_;
	HashTable* ht_;
	// ThreadPool* thrds;
	std::vector<std::thread> th;
	
};
#endif
}


#endif

