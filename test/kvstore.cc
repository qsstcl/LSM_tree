#include "kvstore.h"
#include "utils.h"
#include <string>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <list>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
namespace fs = std::filesystem;
struct KeyOffsetVlen {
    unsigned int time_stamp;
    unsigned long key;
    unsigned long offset;
    unsigned int vlen;
};
bool testBloomFilter(){
    return true;
}
void myPushBack(std::vector<KeyOffsetVlen> &result_vector,KeyOffsetVlen &result)
{
    if (!result_vector.empty()){
        for (auto single_result : result_vector)
        {
            if (single_result.key == result.key && result.time_stamp > single_result.time_stamp)
            {
                single_result = result;
                break; // 找到匹配的键后立即结束循环
            }else if (single_result.key == result.key)
            {
                break;
            }else
            {
                result_vector.push_back(result);
            }

        }
    }else{
        result_vector.push_back(result);
    }


}

void PushBackList(std::vector<KeyOffsetVlen> &result_vector,std::list<std::pair<uint64_t, std::string>> &list,std::ifstream& file)
{
    for (const auto& result : result_vector) {
        // 检查列表中是否已经存在相同的键
        bool keyExists = false;
        for (const auto& pair : list) {
            if (pair.first == result.key) {
                keyExists = true;
                break;
            }
        }

        // 如果列表中不存在相同的键，则将键值对插入到列表中
        if (!keyExists) {
            file.seekg(result.offset);
            std::string value(result.vlen,'\0');
            file.read(&value[0],result.vlen);
            list.push_back(std::make_pair(result.key, value));
        }
    }
    list.sort([](const std::pair<uint64_t, std::string>& a, const std::pair<uint64_t, std::string>& b) {
        return a.first < b.first;
    });
}

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
    this->dir = dir;
    this->vlog = vlog;
    this->sstable_index = 0;
    this->level = 0;
    this->tail = 0;
    this->head = 0;
    std::string sst_folder_name = "../"+dir+"/level0";
    if (fs::create_directories(sst_folder_name))
    {
        std::cout << "succsses creating the folder"<< std::endl;
    }else{
        //the directories already exist
        std::cerr << "failed to creat the folder"<< std::endl;
        int file_count = 0;
        for (const auto& entry : fs::directory_iterator(sst_folder_name)) {
            if (entry.is_regular_file()) {
                ++file_count;
            }
        }
        this->sstable_index = file_count + 1;
    }

    std::string filename = "../data/" + vlog + ".vlog";

    if (fs::exists(filename)) {
        // 文件存在，打开文件
        std::cout << "File exists, opening it..." << std::endl;
        std::ofstream file(filename, std::ios::app); // 追加模式打开文件
        unsigned long file_size = fs::file_size(filename);
        this->head = file_size;
        if (!file.is_open()) {
            std::cerr << "Failed to open file" << std::endl;
            return;
        }
    } else {
        // 文件不存在，创建文件
        std::cout << "File does not exist, creating it..." << std::endl;
        std::ofstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Failed to create file" << std::endl;
            return;
        }
    }

}

KVStore::~KVStore()
{
    saveToVlog();
    delete MemTable;
}
// test if the memtable is oversized
// if oversized, return true
// else return false
bool KVStore::testMemTableSize()
{
    if (MemTable->get_length() < 5)
    {
        return false;
    }else{
        return true;
    }

}
//save memTable data to Vlog
void KVStore::saveToVlog()
{
    std::string filename = "../data/" + vlog + ".vlog";
    std::string sst_filename = "../data/level0/" + std::to_string(sstable_index) + ".sst";
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
    unsigned long time_stamp = sstable_index;
    sstable_index ++;
    unsigned long length = data_to_save.size();
    unsigned long max_key = 0;
    unsigned long min_key = UINT64_MAX;
    for (const auto& pair : data_to_save)
    {
        if (pair.first < min_key)
        {
            min_key = pair.first;
        }
        if (pair.first > max_key)
        {
            max_key = pair.first;
        }
    }

    sst_file.write(reinterpret_cast<const char*>(&time_stamp), sizeof(time_stamp));
    sst_file.write(reinterpret_cast<const char*>(&length), sizeof(length));
    sst_file.write(reinterpret_cast<const char*>(&min_key), sizeof(min_key));
    sst_file.write(reinterpret_cast<const char*>(&max_key), sizeof(max_key));
    char zero_byte = 0;
    for (std::size_t i = 0; i < 8192; ++i) {
        sst_file.write(&zero_byte, sizeof(zero_byte));
    }
    for (const auto& pair : data_to_save) {
        //write data to vlog file
        unsigned int vlen = pair.second.length();
        unsigned long key = pair.first;
        std::string value = pair.second;
        //if it is not deleted
        unsigned long val_offset;

        if (pair.second != "~DELETED~")
        {
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
            val_offset = static_cast<unsigned long>(file.tellp());
            file.write(value.data(), vlen);
        }

        //write data to sst file
        //if it is not deleted
        if (pair.second != "~DELETED~")
        {
            sst_file.write(reinterpret_cast<const char*>(&key), sizeof(key));
            sst_file.write(reinterpret_cast<const char*>(&val_offset), sizeof(val_offset));
            sst_file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
        }else{
            //it is already deleted
            sst_file.write(reinterpret_cast<const char*>(&key), sizeof(key));
            unsigned long invalid_offset = 0;
            sst_file.write(reinterpret_cast<const char*>(&invalid_offset), sizeof(invalid_offset));
            unsigned int invalid_vlen = 0;
            sst_file.write(reinterpret_cast<const char*>(&invalid_vlen), sizeof(invalid_vlen));
        }


    }

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
        delete MemTable;
        MemTable = new skiplist::skiplist_type(0.37);
        put(key,s);
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
        //search in the ssTable level by level
        //need a vector to save data ,because timestamp
        std::vector<KeyOffsetVlen> result_vector;
        for (unsigned int i = 0; i <= level; i++)
        {
            std::string folder_path = "../data/level"+std::to_string(i);

            // 遍历文件夹中的文件
            for (const auto& entry : fs::directory_iterator(folder_path)) {
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().filename().string();
                    std::ifstream sst_file(filename,std::ios::binary);
                    unsigned long time_stamp ;
                    unsigned long key_number ;
                    unsigned long max_key ;
                    unsigned long min_key ;
                    sst_file.read(reinterpret_cast<char*>(&time_stamp), sizeof(time_stamp));
                    sst_file.read(reinterpret_cast<char*>(&key_number), sizeof(key_number));
                    sst_file.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));
                    sst_file.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
                    //if key > max_key or key < min_key ,then go to next sst
                    if (key > max_key || key < min_key)
                    {
                        continue;
                    }
                    //else make a binary search
                    if (testBloomFilter())
                    {
                        //if passed the bloom filter test
                        unsigned long first_key_index = 8224;
                        unsigned long last_key_index = first_key_index + 20 * key_number;
                        unsigned long mid_key;
                        bool is_found = false;
                        while (first_key_index <= last_key_index) {
                            // 计算中间位置
                            unsigned long mid_key_index = (first_key_index + last_key_index) / 2;
                            //get first and last key
                            sst_file.seekg(first_key_index);
                            unsigned long first_key;
                            sst_file.read(reinterpret_cast<char*>(&first_key), sizeof(first_key));
                            sst_file.seekg(last_key_index);
                            unsigned long last_key;
                            sst_file.read(reinterpret_cast<char*>(&last_key), sizeof(last_key));
                            //if (mid_key_index - first_key_index)%20 != 0 ,meaning it does not correspond to a key, complete it

                            if ((mid_key_index - first_key_index)%20 != 0)
                            {
                                mid_key_index += (20-(mid_key_index - first_key_index)%20);
                            }
                            sst_file.seekg(mid_key_index);
                            sst_file.read(reinterpret_cast<char*>(&mid_key), sizeof(mid_key));
                            if (mid_key_index == last_key_index)
                            {
                                if (key == first_key || key == last_key)
                                {
                                    is_found = true;
                                    KeyOffsetVlen result;
                                    result.time_stamp = time_stamp;
                                    if (key == first_key)
                                    {
                                        result.key = first_key;
                                        sst_file.seekg(first_key_index+8);
                                    }else {
                                        result.key = last_key;
                                        sst_file.seekg(last_key_index+8);
                                    }
                                    unsigned long offset;
                                    sst_file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
                                    unsigned int vlen;
                                    sst_file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                                    result.offset = offset;
                                    result.vlen = vlen;
                                    result_vector.push_back(result);
                                    break; ;
                                }else{
                                    break;
                                }
                            }

                            if (mid_key == key)
                            {
                                is_found = true;
                                KeyOffsetVlen result;
                                result.time_stamp = time_stamp;
                                result.key = mid_key;
                                unsigned long offset;
                                sst_file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
                                unsigned int vlen;
                                sst_file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                                result.offset = offset;
                                result.vlen = vlen;
                                result_vector.push_back(result);
                                break;
                            }else if(mid_key < key){
                                first_key_index = mid_key_index + 20;
                            }else{
                                last_key_index = mid_key_index -20;
                            }
                        }
                    }else{
                        //the key must not be in the sst
                        continue;
                    }


                }
            }
        }
        if (result_vector.size() == 0)
        {
            return "";
        }else {
            unsigned long time_stamp = 0;
            unsigned long offset = 0;
            unsigned int vlen = 0;
            for (const auto &result : result_vector)
            {
                if (result.time_stamp > time_stamp)
                {
                    offset = result.offset;
                    vlen = result.vlen;
                    time_stamp = result.time_stamp;
                }

            }
            if (vlen == 0)
            {
                return "";
            }
            std::string filename = "../data/" + vlog + ".vlog";
            std::ifstream file(filename,std::ios::binary);
            file.seekg(offset);
            std::string result(vlen,'\0');
            file.read(&result[0],vlen);
            return result;
        }

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
        //found it in memTable
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
    delete MemTable;
    MemTable = new skiplist::skiplist_type(0.37);
    std::string data_folder = "../data";

    // 检查 data 文件夹是否存在
    if (!fs::exists(data_folder)) {
        std::cerr << "Data folder does not exist" << std::endl;
        return;
    }

    // 删除 data 文件夹及其内容
    try {
        fs::remove_all(data_folder);
        std::cout << "Data folder reset successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to reset data folder: " << e.what() << std::endl;
    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    //scan memTable, put all values between key1 and key2 into list
    MemTable->scan(key1,key2,list);
    std::vector<KeyOffsetVlen> result_vector;
    //scan sst files level by level, encountering the same key ,compare the time_stamp
    for (unsigned int i = 0; i <= level; i++)
    {
        std::string folder_path = "../data/level"+std::to_string(i);

        // 遍历文件夹中的文件
        for (const auto& entry : fs::directory_iterator(folder_path)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().string();
                std::ifstream sst_file(filename,std::ios::binary);
                unsigned long time_stamp ;
                unsigned long key_number ;
                unsigned long min_key ;
                unsigned long max_key ;
                sst_file.read(reinterpret_cast<char*>(&time_stamp), sizeof(time_stamp));
                sst_file.read(reinterpret_cast<char*>(&key_number), sizeof(key_number));
                sst_file.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
                sst_file.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));
                //if search range is beyond this sst,then go to next one
                if (key1 > max_key || key2 < min_key)
                {
                    continue;
                }
                //search range is under this sst
                unsigned long key1_index=0;
                unsigned long key2_index=0;
                unsigned long first_key_index = 8224;
                unsigned long last_key_index = first_key_index + 20 * (key_number-1);
                unsigned long search_key;
                unsigned long search_index = first_key_index;
                //find the key1_index
                while (search_index <= last_key_index)
                {
                    sst_file.seekg(search_index);
                    sst_file.read(reinterpret_cast<char*>(&search_key), sizeof(search_key));
                    if (search_key < key1)
                    {
                        search_index += 20;
                    }else{
                        key1_index = search_index;
                        break;
                    }
                }
                //all the keys in sst are smaller than key1
                if (key1_index == 0)
                {
                    continue;
                }

                //conitinue to find the key2_index
                while (search_index <= last_key_index)
                {
                    sst_file.seekg(search_index);
                    sst_file.read(reinterpret_cast<char*>(&search_key), sizeof(search_key));
                    if (search_key <= key2)
                    {
                        search_index += 20;
                    }else{
                        key2_index = search_index - 20;
                        break;
                    }
                }
                //key2 is beyond range
                if (search_index == last_key_index + 20)
                {
                    key2_index = last_key_index;
                }
                //key2 is smaller than the smallest key in this sst
                if (key2_index == first_key_index - 20)
                {
                    continue;
                }
                sst_file.seekg(key1_index);
                search_index = key1_index;
                while (search_index <= key2_index)
                {
                    KeyOffsetVlen result;
                    result.time_stamp = time_stamp;
                    unsigned long key;
                    sst_file.read(reinterpret_cast<char*>(&key), sizeof(key));
                    unsigned long offset;
                    sst_file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
                    unsigned int vlen;
                    sst_file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                    result.key = key;
                    result.offset = offset;
                    result.vlen = vlen;
                    myPushBack(result_vector,result);
                    search_index += 20;
                }


            }
        }
    }
    std::string filename = "../data/" + vlog + ".vlog";
    std::ifstream file(filename,std::ios::binary);

    PushBackList(result_vector,list,file);

}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}