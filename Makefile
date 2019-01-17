CXXFLAGS += -std=c++14
OBJS = hyKV_test.o HashTable.o Configure.o BplusTree.o BplusTreeList.o PmemKV.o Db.o HiKV.o latency.o SkipList.o

test: $(OBJS)
	g++ -o t $(OBJS) -std=c++14 -O2 -lpthread

HashTable.o: HashTable.h
	g++ -c HashTable.cpp -std=c++14 -O2 -lpthread

Configure.o: Configure.h
	g++ -c Configure.cpp -std=c++14 -O2 -lpthread

Db.o: Db.h Configure.h HashTable.h
	g++ -c Db.cpp -std=c++14 -O2 -lpthread

BplusTree.o: BplusTree.h
	g++ -c BplusTree.cpp -std=c++14 -O2 -lpthread

BplusTreeList.o: BplusTreeList.h
	g++ -c BplusTreeList.cpp -std=c++14 -O2 -lpthread

PmemKV.o: PmemKV.h
	g++ -c PmemKV.cpp -std=c++14 -O2 -lpthread

HiKV.o: HiKV.h
	g++ -c HiKV.cpp -std=c++14 -O2 -lpthread

latency.o: latency.hpp
	g++ -c latency.cpp -std=c++14 -O2 -lpthread

SkipList.o: SkipList.h
	g++ -c SkipList.cc -std=c++14 -O2 -lpthread

hyKV_test.o: microBench.h Configure.h Db.h PmemKV.h HiKV.h
	g++ -c hyKV_test.cc -std=c++14 -O2 -lpthread

clean:
	rm -rf t $(OBJS)


