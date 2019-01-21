//
//  latency.cpp
//  devl
//
//  Created by 吴海源 on 2018/12/29.
//  Copyright © 2018 why. All rights reserved.
//

#include "latency.hpp"

namespace hybridKV {
// following code are referenced from 'Quartz'.
// github: https://github.com/HewlettPackard/quartz

#if defined(__i386__)

static inline unsigned long long asm_rdtsc(void)
{
    unsigned long long int x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}

static inline unsigned long long asm_rdtscp(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"ecx");
    return ( (unsigned long long)lo )|( ((unsigned long long)hi)<<32 );
    
}

#elif defined(__x86_64__)

static inline unsigned long long asm_rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

static inline unsigned long long asm_rdtscp(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"rcx");
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
#else
#error "What architecture is this???"
#endif
/* Flush cacheline */
#define asm_clflush(addr)                   \
({                              \
__asm__ __volatile__ ("clflush %0" :: "m"(*(addr))); \
})

/* Memory fence */
//#define asm_mfence()                \
//({                      \
//PM_FENCE();             \
//__asm__ __volatile__ ("mfence");    \
//})
//
inline hrtime_t cycles_to_ns(int cpu_speed_mhz, hrtime_t cycles)
{
    return (cycles*1000/cpu_speed_mhz);
}

inline hrtime_t ns_to_cycles(int cpu_speed_mhz, hrtime_t ns)
{
    return (ns*cpu_speed_mhz/1000);
}

static int global_cpu_speed_mhz = 0;
static int global_write_latency_ns = 0;
static int global_read_latency_ns = 0;
static int cacheline_size = 64;
//static float cache_hit = 0.20;
static const int emulate_func_latency_ns = 100;

void init_pflush(int cpu_speed_mhz, int write_latency_ns, int read_latency_ns)
{
    global_cpu_speed_mhz = cpu_speed_mhz;
    global_write_latency_ns = write_latency_ns;
    global_read_latency_ns = read_latency_ns;
}
void emulate_latency_ns(uint64_t ns)
{
    hrtime_t cycles;
    hrtime_t start;
    hrtime_t stop;
    start = asm_rdtsc();
    cycles = ns_to_cycles(global_cpu_speed_mhz, ns);
    // LOG("latency" << ns) ;
    do {
        /* RDTSC doesn't necessarily wait for previous instructions to complete
         * so a serializing instruction is usually used to ensure previous
         * instructions have completed. However, in our case this is a desirable
         * property since we want to overlap the latency we emulate with the
         * actual latency of the emulated instruction.
         */
        stop = asm_rdtsc();
        //LOG("now is " << stop);
    } while (stop - start < cycles);
    
}
    
void pload(uint64_t *addr, size_t len) {
    
    if (global_read_latency_ns == 0)
        return;
    len = len + (((uint64_t)addr) & (cacheline_size - 1));
    int64_t to_insert_ns = global_read_latency_ns * ((len+63) / cacheline_size) - emulate_func_latency_ns;
    if (to_insert_ns <= 0) {
        // emulate_latency_ns(0);
        return;
    }
    emulate_latency_ns(to_insert_ns);
}
    
void pflush(uint64_t *addr, size_t len)
{
    if (global_write_latency_ns == 0) {
        return;
    }
    
    /* Measure the latency of a clflush and add an additional delay to
     * meet the latency to write to NVM */
    hrtime_t start;
    hrtime_t stop;
    len = len + (((uint64_t)addr) & (cacheline_size - 1));
    // LOG("flushing");
    start = asm_rdtscp();
    for (uint64_t i=0; i<len; i+=cacheline_size) {
        asm_clflush((uint64_t*)(((uint64_t)addr / 64 *64) + i));
    }
    stop = asm_rdtscp();
    // LOG("flush  done.");
    int64_t to_insert_ns = global_write_latency_ns * ((len+63) / cacheline_size) - cycles_to_ns(global_cpu_speed_mhz, stop-start) - emulate_func_latency_ns;
    if (to_insert_ns <= 0) {
        return;
    }
    // LOG()
    emulate_latency_ns(to_insert_ns);
}
void TimerRDT::setZero() {
    duration_ = 0;
}

TimerRDT::TimerRDT() : duration_(0), lastTime_(0) {

}
TimerRDT::~TimerRDT(){ }

void TimerRDT::start() {
    lastTime_ = asm_rdtscp();
}
void TimerRDT::stop() {
    hrtime_t now = asm_rdtscp();
    duration_ += cycles_to_ns(global_cpu_speed_mhz, now - lastTime_);
}
hrtime_t TimerRDT::lastTime() {
    return lastTime_;
}
hrtime_t TimerRDT::getDuration() {
    // std::cout << "Time consuming " << duration_ / 1000 << " (us)"<< std::endl;
    return duration_/1000;
}

}
