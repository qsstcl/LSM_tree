#include "kvstore.h"
#include <string>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <list>
namespace fs = std::filesystem;
KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
	this->dir = dir;
	this->vlog = vlog;
    std::string sst_folder_name = "../data/"+dir+"/level0";
    if (fs::create_directories(sst_folder_name))
    {
        std::cout << "succsses creating the folder"<< std::endl;
    }else{
        std::cerr << "failed to creat the folder"<< std::endl;
    }

    std::string vlog_folder_name = "../data/"+vlog;
    if (fs::create_directories(vlog_folder_name))
    {
        std::cout << "succsses creating the folder"<< std::endl;
    }else{
        std::cerr << "failed to creat the folder"<< std::endl;
    }
    std::string filename = "../data/vlog/" + vlog + ".vlog";

    // 创建文件流对象并打开文件
    std::ofstream file(filename);

}

KVStore::~KVStore()
{
}
// test if the memtable is oversized 
// if oversized, return true
// else return false
bool KVStore::testMemTableSize()
{
	if (MemTable->get_length() <= 50)
	{
		return false;
	}else{
		return true;
	}
	
}

void KVStore::saveToVlog()
{
	std::string filename = "../data/vlog/" + vlog + ".vlog";
	std::ofstream file(filename);
	std::list<std::pair<unsigned long, std::string>> data_to_save = MemTable->traverse();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	//before inserting,check if it is over 16kB after inserting
	bool is_oversized = testMemTableSize();
	if (!is_oversized)
	{
		MemTable->put(key,s);
	}else{
		saveToVlog();
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string result = MemTable->get(key);
	if (result != "error")
	{
		//found in the MemTable
		if (result!="~DELETED~")
		{
			return result;
		}else{
			return "";
		}
	}else{
		//search in the ssTable
		return "";
	}

}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	//search if this key exists
	std::string search_result = get(key);
	if (search_result != "")
	{
		//found it
		MemTable->put(key,"~DELETED~");
		return true; 
	}else{
		return false;
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}