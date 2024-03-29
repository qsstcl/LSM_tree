#include "kvstore.h"
#include <iostream>
int main(){
    KVStore tree = KVStore("sst","vlog");
    tree.put(1,"hello");
    tree.put(2,"test");
    std::string text = "this";
    tree.put(3,text);
    std::cout<<tree.get(1)<<std::endl;
    std::cout<<tree.get(2)<<std::endl;
    std::cout<<tree.get(3)<<std::endl;

    tree.del(1);
    std::cout<<tree.get(1)<<std::endl;

    return 0;
}