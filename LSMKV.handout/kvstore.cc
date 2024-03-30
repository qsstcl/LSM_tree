#include "kvstore.h"
#include "utils.h"
#include <string>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <list>
#include <vector>
#include <cstdint>
namespace fs = std::filesystem;
KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
    this->dir = dir;
    this->vlog = vlog;
	this->write_vlog_index = 0;
	this->sstable_index = 0;
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
    if (MemTable->get_length() <= 407)
    {
        return false;
    }else{
        return true;
    }

}
//save memTable data to Vlog
void KVStore::saveToVlog()
{
    std::string filename = "../data/vlog/" + vlog + ".vlog";
	std::string sst_filename = "../data/sst/level0" + std::to_string(sstable_index) + ".sst";

    std::ofstream file(filename,std::ios::binary | std::ios::app);
    std::ofstream sst_file(sst_filename,std::ios::binary);
	
    if (!file.is_open()) {
        std::cerr << "无法打开文件" << std::endl;
        return;
    }
	if (!sst_file.is_open())
	{
        std::cerr << "无法打开文件" << std::endl;
        return;
	}
	
    std::list<std::pair<unsigned long, std::string>> data_to_save = MemTable->traverse();
    for (const auto& pair : data_to_save) {
		write_vlog_index ++ ;
        unsigned int vlen = pair.second.length();
        unsigned long key = pair.first;
        std::string value = pair.second;
        file.put(0xff);
        std::vector<unsigned char> binary_data;
        binary_data.reserve(sizeof(unsigned long) + sizeof(unsigned int) + vlen);
        binary_data.insert(binary_data.end(), reinterpret_cast<const unsigned char*>(&key), reinterpret_cast<const unsigned char*>(&key) + sizeof(unsigned long));
        binary_data.insert(binary_data.end(), reinterpret_cast<const unsigned char*>(&vlen), reinterpret_cast<const unsigned char*>(&vlen) + sizeof(unsigned int));
        binary_data.insert(binary_data.end(), pair.second.begin(), pair.second.end());
        // calc the checksum
        uint16_t crc = utils::crc16(binary_data);
        file.write(reinterpret_cast<const char*>(&crc), sizeof(crc));
        file.write(reinterpret_cast<const char*>(&key), sizeof(key));
        file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
        file.write(value.data(), vlen);
    }

}

void KVStore::saveToSSTable()
{

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
		saveToSSTable();
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