

#ifndef TIMER_H
#define TIMER_H
#include <chrono>
class Timer {
public:
    Timer() {
        duration_ = 0;
    }
    ~Timer() { }
    
    void start() {
        lastTime_ = std::chrono::system_clock::now();
    }
    void stop() {
        auto endTime = std::chrono::system_clock::now();
        duration_ += std::chrono::duration_cast<std::chrono::microseconds>(endTime - lastTime_).count();
    }
    
    size_t getDuration() {
        std::cout << "Time consuming " << duration_ << std::endl;
        return duration_;
    }
private:
    std::chrono::system_clock::time_point lastTime_;
    size_t duration_;
};

#endif