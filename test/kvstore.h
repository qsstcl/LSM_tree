#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include <vector>
struct FilenameTimestampKey{
    std::string filename;
    unsigned long timestamp;
    unsigned long min_key;
    unsigned long max_key;
};
class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	skiplist::skiplist_type* MemTable = new skiplist::skiplist_type(0.37);

	std::string dir;

	std::string vlog;

	unsigned long tail;

	unsigned long head;

	unsigned int level;

	unsigned long write_vlog_index;

	unsigned long sstable_index;

	std::string vlog_filename;

	std::string sst_folder_filename;

	std::vector<char> bloom_filter;
public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	bool testMemTableSize();

	void saveToVlogSST();
    
    void mergeSstFiles(std::vector<FilenameTimestampKey>& files_needed_merged,int level);

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	uint64_t GetKeyOffset(uint64_t key);

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

};
