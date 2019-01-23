//
//  DBInterface.h
//  devl
//
//  Created by 吴海源 on 2018/11/7.
//  Copyright © 2018 why. All rights reserved.
//

#ifndef DBInterface_h
#define DBInterface_h

#include <string>
#include <vector>
#include "Tool.h"
namespace hybridKV {
    

class hyDB {
public:
    hyDB() { }
    virtual ~hyDB() { };
    static int Open(hyDB** dbptr, std::string dbname, int size);
    // virtual void rdStartSignal();
    // virtual void rdStopSignal();
//    virtual void showCfg();
    virtual int Get(const std::string& key, std::string* val) = 0;
    virtual int Put(const std::string& key, const std::string& val) = 0;
    virtual int Delete(const std::string& key) = 0;
    virtual int Scan(const std::string& beginKey, int n, std::vector<std::string>& output) = 0;
    virtual int Scan(const std::string& beginKey, const std::string& lastKey, std::vector<std::string>& output) = 0;
    virtual int Update(const std::string& key, const std::string& val) = 0;

    // virtual void debug() = 0;
    // virtual uint64_t time() = 0;
    // virtual void newRound() = 0;
private:
    hyDB(const hyDB&);
    void operator=(const hyDB&);
};
    
}


#endif /* DBInterface_h */
