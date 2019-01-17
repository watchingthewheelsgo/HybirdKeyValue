#ifndef __RANDOM_H
#define __RANDOM_H

namespace hybridKV {
class Random {

public:
	explicit Random(unsigned int sd) : seed_(sd & 0x7ffffffffu) {
		if (seed_ == 0 || seed_ == 0xffffffff)
			seed_ = 1;
	}
	unsigned int Next() {

		static const unsigned int M = 2147483647L;
        static const unsigned int A = 16807;
        seed_ = seed_ & M;
        if (seed_ == 0 || seed_ == M)
            seed_ = 1;
        
        unsigned long long product = seed_ * A;
        seed_ = static_cast<unsigned int>((product >> 31) + (product & M));
        if (seed_ > M)
            seed_ -= M;
        
        return seed_;
	}
	unsigned int Uniform(int n) {
		return Next() % n;
	}

private:
	unsigned int seed_;
};
    
}
#endif
