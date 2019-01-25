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
#include <mutex>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/atomic.hpp>
// #include <boost>
#include "HashTable.h"
#include "kvObject.h"
#include "Configure.h"
#include "DBInterface.h"
#include "BplusTree.h"
#include "Thread.h"
#include "Tool.h"
// #include "Timer.h"
#include "hyKV_def.h"
#include "latency.hpp"

namespace hybridKV {
void* schedule2(void* arg);
	
struct cmdInfo;
class HiKV : public hyDB {
public:
	// typedef std::deque::iterator QITOR;
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
	void clearBGTime() {
		tree_->tmr.setZero();
		tree_->writeCnt = 0;
	}
	uint64_t getBGTime() {
		return tree_->tmr.getDuration();
	}
	void BgInit();
	// void newRound();
	int Recover();
	int Close();
	bool emptyQue() {
		return que.empty();
	}
	int queSize() {
		return qSize;
		// return qSize.load();
	}
	void freePush(cmdInfo* cmd) {
		que2.push(cmd);
		++qSize;
	}
	cmdInfo* freePop() {
		cmdInfo* res;
		// auto res = que2.front();
		que2.pop(res);
		--qSize;
		return res;
	}
	void queue_push(cmdInfo* cmd) {
		mtx_.lock();
		que.push_back(cmd);
		++qSize;
		mtx_.unlock();
	}
	cmdInfo* extraQue() {
		mtx_.lock();
		auto res = que.front();
		que.pop_front();
		--qSize;
		mtx_.unlock();
		return res;
	}
	BplusTreeSplit* tree() {
		return tree_;
	}
	// void debug() {
	//     tree_->showAll();
	// }
	TimerRDT tmr_ht;
	TimerRDT tmr_all;
	bool bgSchedule;
	// static int cnt;
	

private:
	Config* cfg;
	Mutex mtx_;
	// std::mutex mt_;
	std::deque<cmdInfo*> que;
	boost::lockfree::spsc_queue<cmdInfo*, boost::lockfree::capacity<1 << 21>> que2;
	// boost::atomic<bool> done;
	// std::atomic<int> qSize;
	volatile int qSize; // que.size() is not safe when using -O2 opt, just use 'volatile' to gurantee consistancy 
	BplusTreeSplit* tree_;
	HashTable* ht_;
	ThreadPool* thrds;
	// std::vector<std::thread> th;
	
};
}


#endif

