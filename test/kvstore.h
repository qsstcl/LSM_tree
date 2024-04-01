#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
class KVStore : public KVStoreAPI
{
    // You can add your implementation here
private:
    skiplist::skiplist_type* MemTable = new skiplist::skiplist_type(0.37);

    std::string dir;

    std::string vlog;

    unsigned int level;

    unsigned long write_vlog_index;

    unsigned long sstable_index;
public:
    KVStore(const std::string &dir, const std::string &vlog);

    ~KVStore();

    bool testMemTableSize();

    void saveToVlog();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

    void gc(uint64_t chunk_size) override;
};
