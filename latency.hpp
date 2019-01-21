//
//  latency.hpp
//  devl
//
//  Created by 吴海源 on 2018/12/29.
//  Copyright © 2018 why. All rights reserved.
//

#ifndef latency_hpp
#define latency_hpp
#include <stdio.h>
#include "Tool.h"

namespace hybridKV {
typedef uint64_t hrtime_t;
class TimerRDT {
public:
	TimerRDT();
	~TimerRDT();

	void start();
	void stop();
	void setZero();
	hrtime_t lastTime(); 
	hrtime_t getDuration();
private:
    hrtime_t lastTime_;
    hrtime_t duration_;
};
void emulate_latency_ns(uint64_t ns);
    
void pflush(uint64_t *addr, size_t len);
void pload(uint64_t *addr, size_t len);
    
void init_pflush(int cpu_speed_mhz, int write_latency_ns, int read_latency_ns);

}
#endif /* latency_hpp */
