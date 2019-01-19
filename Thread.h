
#ifndef __THREAD_H
#define __THREAD_H

#include <pthread.h>
#include <thread>
#include "hyKV_def.h"
namespace hybridKV {

class ThreadPool {
public:
	typedef void* (BGThread)(void *);
	ThreadPool(uint32_t n) : thrd_nums(n), bgThrds(new pthread_t [n]) { }
    ~ThreadPool() {
        if (bgThrds != nullptr) {
            delete [] bgThrds;
        }
    }
    void CreateJobs(BGThread schedule, void* arg, int i) {
        const int res = pthread_create(&bgThrds[i], NULL, schedule, arg);
        if (res != 0) {
            LOG("creating thread " << i <<" failed.");
        }
        LOG("thread " << i <<" created.");
#ifdef LINUX_CPU_AFFINITY
        // cpu_set_t: This data set is a bitset where each bit represents a CPU.
        cpu_set_t cpuset;
        // CPU_ZERO: This macro initializes the CPU set set to be the empty set.
        CPU_ZERO(&cpuset);
        // CPU_SET: This macro adds cpu to the CPU set set.
        CPU_SET(i, &cpuset);
        // pthread_setaffinity_np: The pthread_setaffinity_np() function sets the CPU affinity mask of the thread thread to the CPU set pointed to by cpuset. If the call is successful, and the thread is not currently running on one of the CPUs in cpuset, then it is migrated to one of those CPUs.
        const int set_result = pthread_setaffinity_np(bgThrds[i], sizeof(cpu_set_t), &cpuset);
        if (set_result != 0) {
            LOG("pthread_setaffinity_np Failed.");
        }
#endif
    }
private:
	uint32_t thrd_nums;
	// BGThread bg_schedule;
	pthread_t *bgThrds;

};

class Mutex {
public:
 	Mutex() {
 		pthread_mutex_init(&mtx, NULL);
 	}
 	~Mutex() {
        pthread_mutex_destroy(&mtx);
 	}
 	void Lock() {
 		pthread_mutex_lock(&mtx);
 	}
 	void unLock() {
 		pthread_mutex_unlock(&mtx);
 	}

private:
	pthread_mutex_t mtx;
	Mutex(const Mutex&);
	void operator=(const Mutex&);
};
    
}
#endif
