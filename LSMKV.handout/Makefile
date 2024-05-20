LINK.o = $(LINK.cc)
CXXFLAGS = -O3 -std=c++20 -Wall

all: correctness persistence test

correctness: kvstore.o correctness.o skiplist.o
	$(CXX) $(CXXFLAGS) -o correctness kvstore.o correctness.o skiplist.o

persistence: kvstore.o persistence.o skiplist.o
	$(CXX) $(CXXFLAGS) -o persistence kvstore.o persistence.o skiplist.o

test: kvstore.o test.o skiplist.o
	$(CXX) $(CXXFLAGS) -o test kvstore.o test.o skiplist.o

kvstore.o: kvstore.cc kvstore.h
	$(CXX) $(CXXFLAGS) -c kvstore.cc

correctness.o: correctness.cc kvstore.h skiplist.h
	$(CXX) $(CXXFLAGS) -c correctness.cc

test.o: test.cc kvstore.h skiplist.h
	$(CXX) $(CXXFLAGS) -c test.cc

persistence.o: persistence.cc kvstore.h skiplist.h
	$(CXX) $(CXXFLAGS) -c persistence.cc

skiplist.o: skiplist.cc skiplist.h
	$(CXX) $(CXXFLAGS) -c skiplist.cc

clean:
	-rm -f correctness persistence test *.o

