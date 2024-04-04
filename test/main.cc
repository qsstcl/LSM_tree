#include <iostream>
#include <cstdint>
#include <string>
#include "kvstore.h"
int main()
{
    std::string data = "data";
    std::string vlog = "vlog";
    KVStore tree(data,vlog);
    //test put func
    tree.put(1,"1");
    tree.put(2,"2");
    tree.put(3,"3");
    tree.put(4,"4");
    tree.put(5,"5");
    tree.put(6,"6");
    tree.put(7,"7");
    tree.put(8,"8");
    tree.put(9,"9");
    tree.put(10,"10");
    std::string s = "s";
    tree.put(1,s);
    std::list<std::pair<uint64_t, std::string>> result;
    tree.scan(2,8,result);

	return 0;
}
