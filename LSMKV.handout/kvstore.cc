#include "kvstore.h"
#include "utils.h"
#include "MurmurHash3.h"
#include <string>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <list>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <complex>
#include <queue>
#include <random>


namespace fs = std::filesystem;
void deleteFilesInFolder(const std::string& folderPath) {
    if (fs::exists(folderPath)) {
        try {
            for (const auto& entry : fs::directory_iterator(folderPath)) {
                if (entry.is_regular_file()) {
                    fs::remove(entry.path());
                } else if (entry.is_directory()) {
                    deleteFilesInFolder(entry.path());
                    fs::remove(entry.path());
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Failed to delete files in folder " << folderPath << ": " << e.what() << std::endl;
        }
    }
}

int countLevels(const std::string& filepath) {
    size_t pos = filepath.find("level");
    if (pos == std::string::npos) {
        // 如果找不到 "level"，说明路径格式不正确
        return -1;
    }

    int levels = 0;
    pos += 5; // "level" 后面有5个字符
    int pos_of_end = pos;
    while ( filepath[pos_of_end] != '/' && pos_of_end < filepath.length()) {
        pos_of_end ++;
    }
    levels = stoi(filepath.substr(pos, pos_of_end));
    return levels;
}


struct KeyOffsetVlen {
    unsigned int time_stamp;
    unsigned long key;
    unsigned long offset;
    unsigned int vlen;
};

struct BufferptrTimestamp
{
    char* bufferptr;
    unsigned long timestamp;
    unsigned long size;
    unsigned long read_position;
    unsigned long reading_key;
    int level;
    friend bool operator<(BufferptrTimestamp f1, BufferptrTimestamp f2) {
        if (f1.reading_key == f2.reading_key && f1.timestamp == f2.timestamp){
            return f1.level < f2.level;
        }
        if (f1.reading_key == f2.reading_key) {
            return f1.timestamp > f2.timestamp; // 如果key相等，则保留时间戳更大的那一个
        }
        return f1.reading_key > f2.reading_key;
    }
    BufferptrTimestamp(char* input,unsigned long input2){
        bufferptr = input;
        timestamp = input2;
    }
};
// 自定义比较函数，用于排序
bool compareFiles(const FilenameTimestampKey& a, const FilenameTimestampKey& b) {
    if (a.timestamp != b.timestamp) {
        return a.timestamp < b.timestamp; // 按照时间戳从小到大排序
    } else {
        return a.min_key < b.min_key; // 当时间戳相等时，按照 min_key 从小到大排序
    }
}
//test if key exist in this bloom filter
bool testBloomFilter(char * file_bloom_filter,uint64_t key){
    std::uint32_t hashValue[4];
    MurmurHash3_x64_128(&key, sizeof(key), 1, hashValue);
    for (int j = 0; j < 4; ++j) {
        int index = hashValue[j] % 8192;
        if (!file_bloom_filter[index]) {
            return false;
        }
    }
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
                break;
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

//func to merge sst files and write them to the level specified
void KVStore::mergeSstFiles(std::vector<FilenameTimestampKey>& files_needed_merged,int level)
{
    std::priority_queue<BufferptrTimestamp> pq;
    unsigned long result_size = 0;
    std::vector<BufferptrTimestamp> buffer_vec;
    unsigned long max_timestamp = 0;
    for (const auto& single_file : files_needed_merged) {
        std::ifstream sst_file(single_file.filename, std::ios::binary);
        if (!sst_file.is_open()) {
            std::cerr << "from mergeSst\n";
            std::cerr << "Error opening file: " << single_file.filename << std::endl;
            return;
        }
        // 获取文件大小
        sst_file.seekg(0, std::ios::end);
        std::streampos file_size = sst_file.tellg();
        sst_file.seekg(0, std::ios::beg);
        result_size += file_size;
        // 读取文件内容到内存缓冲区
        char* buffer = new char[file_size];
        if (!sst_file.read(buffer, file_size)) {
            std::cerr << "Error reading file: " << single_file.filename << std::endl;
            delete[] buffer;
            return;
        }
        BufferptrTimestamp this_file_buffer(buffer, single_file.timestamp);
        if (single_file.timestamp > max_timestamp) max_timestamp = single_file.timestamp;
        this_file_buffer.level = countLevels(single_file.filename);
        this_file_buffer.size = file_size;
        this_file_buffer.read_position = 8224;
        this_file_buffer.reading_key = *(unsigned long*)(buffer + this_file_buffer.read_position);
        this_file_buffer.read_position += 20;
        pq.push(this_file_buffer);
        buffer_vec.push_back(this_file_buffer);
    }
    
    char* result = new char[result_size];
    unsigned long keys_num = 0;
    unsigned long current_position = 0;
    while (!pq.empty()) {
        BufferptrTimestamp current_file = pq.top();
        pq.pop();
        while (pq.size()>0 && current_file.reading_key == pq.top().reading_key)
        {
            if (current_file.read_position < current_file.size) {
                current_file.reading_key = *(unsigned long*)(current_file.bufferptr+current_file.read_position);
                current_file.read_position += 20;
                pq.push(current_file);
                current_file = pq.top();
                pq.pop();
            }else{
                //current file already finished reading
                current_file = pq.top();
                pq.pop();
            }
        }
        std::memcpy(result + current_position, current_file.bufferptr + current_file.read_position-20, 20);
        keys_num += 1;
        current_position += 20;
        if (current_file.read_position < current_file.size) {
            current_file.reading_key = *(unsigned long*)(current_file.bufferptr + current_file.read_position);
            current_file.read_position += 20;
            pq.push(current_file); // 如果当前文件未读取完毕，则重新放入优先队列
        }
    }
    // 保存合并后的文件
    // your code to write the merged content to the result file goes here
    // get first 400 keys
    for (int i = 0; i < keys_num / 400; ++i) {
        //write into several 400 keys sst files
        char * bloom_filter = new char[8192];
        unsigned long timestamp = max_timestamp;
        unsigned long fixed_num = 400;
        unsigned long min_key = *(unsigned long*)(result + 8000*i);
        unsigned long max_key = *(unsigned long*)(result + 8000*i + 7980);
        for (int j = 0; j < 400; ++j) {
            std::uint32_t hashValue[4];
            unsigned long current_key = *(unsigned long*)(result + 8000*i + 20*j);
            MurmurHash3_x64_128(&current_key, sizeof(current_key), 1, hashValue);
            for (int j = 0; j < 4; ++j) {
                int index = hashValue[j] % 8192;
                bloom_filter[index] = 1;
            }
        }
        if (!utils::dirExists(sst_folder_filename+"/level"+std::to_string(level)))
        {
            utils::mkdir(sst_folder_filename+"/level"+std::to_string(level));
        }
        std::string sst_filename = sst_folder_filename+"/level" + std::to_string(level) + "/" + "merged[" + std::to_string(i) + "]" + std::to_string(sstable_index-1) + ".sst";
        std::ofstream sst_file(sst_filename,std::ios::binary);
        sst_file.write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
        sst_file.write(reinterpret_cast<const char*>(&fixed_num), sizeof(fixed_num));
        sst_file.write(reinterpret_cast<const char*>(&min_key), sizeof(min_key));
        sst_file.write(reinterpret_cast<const char*>(&max_key), sizeof(max_key));
        sst_file.write(bloom_filter,8192);
        sst_file.write(result+8000*i,8000);
        delete[] bloom_filter;
    }
    unsigned files_400_num = keys_num/400;
    //deal with keys_num%400 case
    //write into several 400 keys sst files
    if (keys_num%400!=0){
        char * bloom_filter = new char[8192];
        unsigned long timestamp = max_timestamp;
        unsigned long left_keys_num = keys_num % 400;
        unsigned long min_key = *(unsigned long*)(result + files_400_num*8000);
        unsigned long max_key = *(unsigned long*)(result + files_400_num*8000 + 20*(left_keys_num-1));
        for (int j = 0; j < keys_num % 400; ++j) {
            std::uint32_t hashValue[4];
            unsigned long current_key = *(unsigned long*)(result + 8000*files_400_num + 20*j);
            MurmurHash3_x64_128(&current_key, sizeof(current_key), 1, hashValue);
            for (int j = 0; j < 4; ++j) {
                int index = hashValue[j] % 8192;
                bloom_filter[index] = 1;
            }
        }
        if (!utils::dirExists(sst_folder_filename+"/level"+std::to_string(level)))
        {
            utils::mkdir(sst_folder_filename+"/level"+std::to_string(level));
        }
        std::string sst_filename = sst_folder_filename+"/level" + std::to_string(level) + "/" + "merged[" + std::to_string(files_400_num) + "]" + std::to_string(sstable_index-1) + ".sst";
        std::ofstream sst_file(sst_filename,std::ios::binary);
        sst_file.write(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
        sst_file.write(reinterpret_cast<const char*>(&left_keys_num), sizeof(left_keys_num));
        sst_file.write(reinterpret_cast<const char*>(&min_key), sizeof(min_key));
        sst_file.write(reinterpret_cast<const char*>(&max_key), sizeof(max_key));
        sst_file.write(bloom_filter,8192);
        sst_file.write(result+8000*files_400_num,20*(keys_num%400));
        delete[] bloom_filter;

    }

    // Clean up
    delete[] result;
    for (auto& file : buffer_vec) {
        delete[] file.bufferptr;
    }
}

void deleteMergedFiles(std::vector<FilenameTimestampKey>& merged_files){
    for (auto file:merged_files)
    {
        utils::rmfile(file.filename);
    }
    
}
KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
    this->dir = dir;
    this->vlog = vlog;
	this->sstable_index = 0;
    this->level = 0;
    this->tail = 0;
    this->head = 0;
    this->vlog_filename = "./"+ vlog + ".vlog";
    this->sst_folder_filename = "./"+dir;
    bloom_filter.resize(8192);
    bloom_filter.assign(8192, false);
    
    unsigned sst_num = 0;
    if (utils::dirExists(sst_folder_filename)){
        //the directories already exist
        bool isFound = false;
        for (const auto& entry : fs::directory_iterator(sst_folder_filename)) {
            if (entry.is_directory())
            {
                if (entry.path().filename() =="level0")
                {
                    for (const auto& sst : fs::directory_iterator(entry)) {
                        if (sst.is_regular_file())
                        {
                            std::string file_name = sst.path().filename();
                            std::string num;
                            unsigned index = file_name.find('.');
                            num = file_name.substr(0,index);
                            if (std::stoi(num)>sst_num){
                                sst_num = std::stoi(num);
                                isFound = true;
                            }
                        }
                    }
                }
                this->level++;
            }
        }

        if (!isFound){
            for (const auto& entry : fs::directory_iterator(sst_folder_filename)) {
                if (entry.is_directory())
                {
                    if (entry.path().filename() =="level1")
                    {
                        for (const auto& sst : fs::directory_iterator(entry)) {
                            if (sst.is_regular_file())
                            {
                                std::string file_name = sst.path().filename();
                                std::string num;
                                unsigned index2 = file_name.find('.');
                                unsigned index1 = file_name.find(']')+1;
                                num = file_name.substr(index1,index2-index1);
                                if (std::stoi(num)>sst_num){
                                    sst_num = std::stoi(num);
                                    isFound = true;
                                }
                            }
                        }
                    }
                    this->level++;
                }
            }
        }
        this->sstable_index = sst_num + 1;
    }else
    {
        utils::mkdir(sst_folder_filename);
    }
    

    if (fs::exists(vlog_filename)) {
        // 文件存在，打开文件
        std::cout << "File exists, opening it..." << std::endl;
        std::ifstream file(vlog_filename, std::ios::app); // 追加模式打开文件
        unsigned long file_size = fs::file_size(vlog_filename);
        this->head = file_size;
        if (!file.is_open()) {
            std::cerr << "Failed to open file" << std::endl;
            return;
        }
        char single_byte = 0;
        unsigned long offset = utils::seek_data_block(vlog_filename);
        file.seekg(offset);
        while (single_byte != -1)
        {
            file.read(reinterpret_cast<char*>(&single_byte), sizeof(single_byte));
        }
        uint16_t Checksum;
        uint16_t crc;
        this->tail=file.tellg()-1;
        do
        {
            file.read(reinterpret_cast<char*>(&Checksum), sizeof(Checksum));
            uint64_t Key;
            file.read(reinterpret_cast<char*>(&Key), sizeof(Key));
            uint32_t vlen;
            file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
            std::string result(vlen,'\0');
            file.read(&result[0],vlen);
            std::vector<unsigned char> binary_data;
            binary_data.reserve(sizeof(unsigned long) + sizeof(unsigned int) + vlen);
            binary_data.insert(binary_data.end(), reinterpret_cast<const unsigned char*>(&Key), reinterpret_cast<const unsigned char*>(&Key) + sizeof(unsigned long));
            binary_data.insert(binary_data.end(), reinterpret_cast<const unsigned char*>(&vlen), reinterpret_cast<const unsigned char*>(&vlen) + sizeof(unsigned int));
            binary_data.insert(binary_data.end(), result.begin(), result.end());
            // calc the checksum
            crc = utils::crc16(binary_data);
            if (crc != Checksum) this->tail = file.tellg();
        } while (Checksum!=crc);


    } else {
        // 文件不存在，创建文件
        std::cout << "File does not exist, creating it..." << std::endl;
        std::ofstream file(vlog_filename);
        if (!file.is_open()) {
            std::cerr << "Failed to create file" << std::endl;
            return;
        }
    }

}

KVStore::~KVStore()
{
    if(this->MemTable->get_length())
    {
        saveToVlogSST();
    }
    // 写入 sst_index 到文件
    delete MemTable;
}
// test if the memtable is oversized
// if oversized, return true
// else return false
bool KVStore::testMemTableSize()
{
    if (MemTable->get_length() < 400)
    {
        return false;
    }else{
        return true;
    }

}
//save memTable data to Vlog
void KVStore::saveToVlogSST()
{
    //********write header to file
    std::vector<std::basic_string<char>> ret;
    int level0_filenum = 0;

    if (!utils::dirExists(sst_folder_filename+"/level0"))
        utils::mkdir(sst_folder_filename+"/level0");
    if(true)
    {

        std::string sst_filename = sst_folder_filename+"/level0/" + std::to_string(sstable_index) + ".sst";
        std::ofstream file(vlog_filename,std::ios::binary | std::ios::app);
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

        //*******write header to file

        //**write BloomFilter to file
        /**
        * Example
        long long key = 103122;
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        */
        for (auto keyValuePair : data_to_save)
        {
            std::uint32_t hashValue[4];
            MurmurHash3_x64_128(&keyValuePair.first, sizeof(keyValuePair.first), 1, hashValue);
            for (int j = 0; j < 4; ++j) {
                int index = hashValue[j] % 8192;
                bloom_filter[index] = 1;
            }
        }
        for (auto value : bloom_filter) {
            sst_file.write(&value, sizeof(value));
        }

        //******write BloomFilter to sst file

        //******write <Key,Offset,Vlen>to sst file, <Magic,Checksum,Key,Vlen,Valut>to vlog files
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
                head += 15 + vlen;
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
        //******write <Key,Offset,Vlen>to sst file
    }
    level0_filenum = utils::scanDir(sst_folder_filename+"/level0",ret);
    if(level0_filenum>=3){
        int level = 0;
        //compact to next level
        int level_filenum = 0;
        int next_level_filenum = 0;
        do{
            ret.clear();
            level_filenum = utils::scanDir(sst_folder_filename+"/level"+std::to_string(level),ret);
            int files_need_to_be_compacted;
            if (level == 0)
            {
                files_need_to_be_compacted = 3;
            }else{
                files_need_to_be_compacted = level_filenum - std::pow(2,(level+1));
            }
            //gather data from this level
            //0.get all the timestamps of sst_files of this level, determine which ones need to be merged
            //1.statics all the keys range these sst_files cover
            //2.find all the sst_files falling within this range in next level
            //3.merge them together and split into new sst_files per 16kB, write these new sst files to next level
            //4.if the sst_files number in the next level exceeds,continue to pick several files to continue this process
            int min_key = INT32_MAX, max_key=0;

            std::vector<FilenameTimestampKey> files_thislevel_vec;
            for (std::string single_filename:ret) {
                std::ifstream file("./data/level"+std::to_string(level)+"/"+single_filename, std::ios::binary);
                if (!file.is_open()) {
                    std::cerr << "from Saveto\n";
                    std::cerr << "Error opening file: " << single_filename << std::endl;
                    continue;
                }
                FilenameTimestampKey filenameTimestampKey;
                // 读取文件中的时间戳
                unsigned long timestamp;
                file.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
                unsigned long key_num;
                file.read(reinterpret_cast<char*>(&key_num), sizeof(key_num));
                unsigned long min_key;
                file.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
                unsigned long max_key;
                file.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));
                filenameTimestampKey.filename = "./data/level"+std::to_string(level)+"/"+single_filename;
                filenameTimestampKey.timestamp = timestamp;
                filenameTimestampKey.min_key = min_key;
                filenameTimestampKey.max_key = max_key;
                files_thislevel_vec.push_back(filenameTimestampKey);
                // 关闭文件
                file.close();
            }
            std::vector<FilenameTimestampKey> files_needed_merged;
            std::sort(files_thislevel_vec.begin(), files_thislevel_vec.end(), compareFiles);
            for (int i = 0; i < files_need_to_be_compacted; ++i) {
                if (files_thislevel_vec[i].min_key < min_key){
                    min_key = files_thislevel_vec[i].min_key;
                }
                if (files_thislevel_vec[i].max_key > max_key){
                    max_key = files_thislevel_vec[i].max_key;
                }
                files_needed_merged.push_back(files_thislevel_vec[i]);
            }
            if(utils::dirExists(sst_folder_filename+"/level"+std::to_string(level+1)))
            {
                std::vector<FilenameTimestampKey> files_nextlevel_vec;
                ret.clear();
                int next_level_filenum = utils::scanDir(sst_folder_filename+"/level"+std::to_string(level+1),ret);
                for (auto single_nextlevel_file:ret)
                {
                    std::ifstream file("./data/level"+std::to_string(level+1)+"/"+single_nextlevel_file, std::ios::binary);
                    if (!file.is_open()) {
                        std::cerr << "from save to next level\n";

                        std::cerr << "Error opening file: " << single_nextlevel_file << std::endl;
                        continue;
                    }
                    FilenameTimestampKey filenameTimestampKey;
                    // 读取文件中的时间戳
                    unsigned long timestamp;
                    file.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
                    unsigned long key_num;
                    file.read(reinterpret_cast<char*>(&key_num), sizeof(key_num));
                    unsigned long min_key;
                    file.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
                    unsigned long max_key;
                    file.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));
                    filenameTimestampKey.filename = "./data/level"+std::to_string(level+1)+"/"+single_nextlevel_file;
                    filenameTimestampKey.timestamp = timestamp;
                    filenameTimestampKey.min_key = min_key;
                    filenameTimestampKey.max_key = max_key;
                    files_nextlevel_vec.push_back(filenameTimestampKey);
                    // 关闭文件
                    file.close();
                }
                for (auto each_file:files_nextlevel_vec)
                {
                    //test overlap
                    if (!(each_file.max_key< min_key||each_file.min_key>max_key))
                    {
                        files_needed_merged.push_back(each_file);
                    }
                }
                mergeSstFiles(files_needed_merged,level+1);
                deleteMergedFiles(files_needed_merged);
            }else{
                //next level has no files
                utils::mkdir(sst_folder_filename+"/level"+std::to_string(level+1));
                mergeSstFiles(files_needed_merged,level+1);
                deleteMergedFiles(files_needed_merged);
                
            }
            level += 1;
            ret.clear();
            next_level_filenum = utils::scanDir(sst_folder_filename+"/level"+std::to_string(level),ret);
        } while (next_level_filenum > std::pow(2,(level+1)));
        if (level > this->level)
        {
            this->level = level;
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
        saveToVlogSST();
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
    if (result != "error!")
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
            if (!utils::dirExists("./data/level" + std::to_string(i))) return "";
            std::string folder_path = "./data/level"+std::to_string(i);

            // 遍历文件夹中的文件
            for (const auto& entry : fs::directory_iterator(folder_path)) {
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().string();
                    std::ifstream sst_file(filename,std::ios::binary);
                    // 获取文件大小
                    sst_file.seekg(0, std::ios::end);
                    std::streampos file_size = sst_file.tellg();
                    sst_file.seekg(0, std::ios::beg);

                    // 读取文件内容到内存缓冲区
                    char* buffer= new char[file_size];
                    if (!sst_file.read(buffer, file_size)) {
                        std::cerr << "Error reading file\n";
                        return "wrong";
                    }
                    unsigned long read_index = 0;
                    unsigned long time_stamp = ((unsigned long*)buffer)[0];
                    unsigned long key_number = ((unsigned long*)buffer)[1];
                    unsigned long min_key = ((unsigned long*)buffer)[2];
                    unsigned long max_key = ((unsigned long*)buffer)[3];
                    //if key > max_key or key < min_key ,then go to next sst
                    if (key > max_key || key < min_key)
                    {
                        delete[] buffer;
                        continue;
                    }
                    //else make a binary search
                    //get a bool vector
                    char byte;
                    char * file_bloom_filter = new char[8192];
                    memcpy(file_bloom_filter, buffer + 32, 8192);

                    if (testBloomFilter(file_bloom_filter,key))
                    {
                        //if passed the bloom filter test
                        unsigned long first_key_index = 8224;
                        unsigned long last_key_index = first_key_index + 20 * (key_number-1);
                        unsigned long mid_key;
                        bool is_found = false;
                        while (first_key_index <= last_key_index) {
                            // 计算中间位置
                            unsigned long mid_key_index = (first_key_index + last_key_index) / 2;
                            //get first and last key
                            unsigned long first_key = *((unsigned long*)(buffer + first_key_index));
                            unsigned long last_key = *((unsigned long*)(buffer + last_key_index));
                            //if (mid_key_index - first_key_index)%20 != 0 ,meaning it does not correspond to a key, complete it
                            if ((mid_key_index - first_key_index)%20 != 0)
                            {
                                mid_key_index += (20-(mid_key_index - first_key_index)%20);
                            }
                            mid_key = *((unsigned long*)(buffer + mid_key_index));
                            read_index = mid_key_index+8;
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
                                        read_index = first_key_index +8;
                                    }else {
                                        result.key = last_key;
                                        read_index = last_key_index + 8;
                                    }
                                    unsigned long offset = *((unsigned long*)(buffer+read_index));
                                    read_index+=8;
                                    unsigned int vlen = *((unsigned int*)(buffer+read_index));
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
                                unsigned long offset = *((unsigned long*)(buffer+read_index));
                                read_index += 8;
                                unsigned int vlen = *((unsigned int*)(buffer+read_index));
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
                        //clear memory
                        delete[] buffer;
                        delete[] file_bloom_filter;
                    }else{
                        //the key must not be in the sst
                        delete[] buffer;
                        delete[] file_bloom_filter;
                        continue;
                    }


                }
            }
            if (!result_vector.empty())
            {
                //found in higher levels
                goto out1;
            }
            
        }
        out1:
        if (result_vector.size() == 0)
        {
            return "";
        }else {
            unsigned long time_stamp = 0;
            unsigned long offset = 0;
            unsigned int vlen = 0;
            for (const auto &result : result_vector)
            {
                if (result.time_stamp >= time_stamp)
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
            std::ifstream file(vlog_filename,std::ios::binary);
            if (!file.is_open())
            {
                std::cerr<< "error when opening vlog files\n";
                return "";
            }
            file.seekg(offset);
            std::string result(vlen,'\0');
            file.read(&result[0],vlen);
            file.close();
            return result;
        }
        
    }

}

/*
 *find the value valid newest offset corresponding to key
 *return the offset ,if this key is currently in Memtable,return 0
 *return 0,indicating that it does not need to insert again
 */
uint64_t KVStore::GetKeyOffset(uint64_t key){
    std::string result = MemTable->get(key);
    if (result != "error!")
    {
        return 0;
    }else{
        //search in the ssTable level by level
        //need a vector to save data ,because timestamp
        std::vector<KeyOffsetVlen> result_vector;
        for (unsigned int i = 0; i <= level; i++)
        {
            std::string folder_path = "./data/level"+std::to_string(i);

            // 遍历文件夹中的文件
            for (const auto& entry : fs::directory_iterator(folder_path)) {
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().string();
                    std::ifstream sst_file(filename,std::ios::binary);
                    // 获取文件大小
                    sst_file.seekg(0, std::ios::end);
                    std::streampos file_size = sst_file.tellg();
                    sst_file.seekg(0, std::ios::beg);

                    // 读取文件内容到内存缓冲区
                    char* buffer= new char[file_size];
                    if (!sst_file.read(buffer, file_size)) {
                        std::cerr << "Error reading file\n";
                        return 0;
                    }
                    unsigned long read_index = 0;
                    unsigned long time_stamp = ((unsigned long*)buffer)[0];
                    unsigned long key_number = ((unsigned long*)buffer)[1];
                    unsigned long min_key = ((unsigned long*)buffer)[2];
                    unsigned long max_key = ((unsigned long*)buffer)[3];
                    //if key > max_key or key < min_key ,then go to next sst
                    if (key > max_key || key < min_key)
                    {
                        delete[] buffer;
                        continue;
                    }
                    //else make a binary search
                    //get a bool vector
                    char byte;
                    char * file_bloom_filter = new char[8192];
                    memcpy(file_bloom_filter, buffer + 32, 8192);

                    if (testBloomFilter(file_bloom_filter,key))
                    {
                        //if passed the bloom filter test
                        unsigned long first_key_index = 8224;
                        unsigned long last_key_index = first_key_index + 20 * (key_number-1);
                        unsigned long mid_key;
                        bool is_found = false;
                        while (first_key_index <= last_key_index) {
                            // 计算中间位置
                            unsigned long mid_key_index = (first_key_index + last_key_index) / 2;
                            //get first and last key
                            unsigned long first_key = *((unsigned long*)(buffer + first_key_index));
                            unsigned long last_key = *((unsigned long*)(buffer + last_key_index));
                            //if (mid_key_index - first_key_index)%20 != 0 ,meaning it does not correspond to a key, complete it
                            if ((mid_key_index - first_key_index)%20 != 0)
                            {
                                mid_key_index += (20-(mid_key_index - first_key_index)%20);
                            }
                            mid_key = *((unsigned long*)(buffer + mid_key_index));
                            read_index = mid_key_index+8;
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
                                        read_index = first_key_index +8;
                                    }else {
                                        result.key = last_key;
                                        read_index = last_key_index + 8;
                                    }
                                    unsigned long offset = *((unsigned long*)(buffer+read_index));
                                    read_index+=8;
                                    unsigned int vlen = *((unsigned int*)(buffer+read_index));
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
                                unsigned long offset = *((unsigned long*)(buffer+read_index));
                                read_index += 8;
                                unsigned int vlen = *((unsigned int*)(buffer+read_index));
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
                        //clear memory
                        delete[] buffer;
                        delete[] file_bloom_filter;
                    }else{
                        //the key must not be in the sst
                        delete[] buffer;
                        delete[] file_bloom_filter;
                        continue;
                    }


                }
            }
            if (!result_vector.empty())
            {
                //found in the higher levels
                goto out;
            }
            
        }
        
        out:
        if (result_vector.size() == 0)
        {
            return 0;
        }else {
            unsigned long time_stamp = 0;
            unsigned long offset = 0;
            unsigned int vlen = 0;
            for (const auto &result : result_vector)
            {
                if (result.time_stamp >= time_stamp)
                {
                    offset = result.offset;
                    vlen = result.vlen;
                    time_stamp = result.time_stamp;
                }
            }
            if (vlen == 0)
            {
                return 0;
            }else{
                return offset;
            }
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
        if (!testMemTableSize())
        {
            MemTable->put(key,"~DELETED~");
        }else {
            saveToVlogSST();
            delete MemTable;
            MemTable = new skiplist::skiplist_type(0.37);
            MemTable->put(key,"~DELETED~");
        }
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
    std::string data_folder = "./data";
    std::string level0_folder = data_folder + "/level0";
    this->sstable_index = 0;
    this->tail=0;
    this->head=0;
    try {
        // 删除整个文件夹及其内容
        fs::remove_all(data_folder);
        std::cout << "Deleted data folder and its contents successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to delete data folder: " << e.what() << std::endl;
    }
    this->level = 0;

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
        if (!utils::dirExists("./data/level0")) return ;
        std::string folder_path = "./data/level"+std::to_string(i);

        // 遍历文件夹中的文件
        for (const auto& entry : fs::directory_iterator(folder_path)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().string();
                std::ifstream sst_file(filename,std::ios::binary);
                sst_file.seekg(0,std::ios::end);
                std::streampos file_size = sst_file.tellg();
                sst_file.seekg(0,std::ios::beg);
                char* buffer = new char[file_size];
                if (!sst_file.read(buffer, file_size)) {
                    std::cerr << "Error reading file: " << filename << std::endl;
                    delete[] buffer;
                    return;
                }
                unsigned long read_index = 0;
                unsigned long time_stamp = ((unsigned long*)buffer)[0] ;
                unsigned long key_number = ((unsigned long*)buffer)[1] ;
                unsigned long min_key = ((unsigned long*)buffer)[2] ;
                unsigned long max_key = ((unsigned long*)buffer)[3]  ;
                //if search range is beyond this sst,then go to next one
                if (key1 > max_key || key2 < min_key)
                {
                    delete[] buffer;
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
                    search_key = * ((unsigned long*)(buffer + search_index));
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
                    delete[] buffer;
                    continue;
                }
                
                //conitinue to find the key2_index
                while (search_index <= last_key_index)
                {
                    search_key = * ((unsigned long*)(buffer + search_index));
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
                    delete[] buffer;
                    continue;
                }
                search_index = key1_index;
                while (search_index <= key2_index)
                {
                    KeyOffsetVlen result;
                    result.time_stamp = time_stamp;
                    unsigned long key = *((unsigned long*)(buffer + search_index));
                    search_index += 8;
                    unsigned long offset = *((unsigned long*)(buffer + search_index));
                    search_index += 8;
                    unsigned int vlen = *((unsigned int*)(buffer + search_index));
                    search_index += 4;
                    result.key = key;
                    result.offset = offset;
                    result.vlen = vlen;
                    myPushBack(result_vector,result);
                }
                delete[] buffer;
                
            }
        }
    }

    std::ifstream file(vlog_filename,std::ios::binary);

    PushBackList(result_vector,list,file);

}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
    uint64_t origin_tail = tail;
    std::ifstream file(vlog_filename,std::ios::binary);
    file.seekg(tail);
    uint64_t collected_space=0;
    while (collected_space < chunk_size)
    {
        char Magic;
        file.read(reinterpret_cast<char*>(&Magic), sizeof(Magic));
        uint16_t Checksum;
        file.read(reinterpret_cast<char*>(&Checksum), sizeof(Checksum));
        uint64_t Key;
        file.read(reinterpret_cast<char*>(&Key), sizeof(Key));
        uint32_t vlen;
        file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
        uint64_t val_offset = static_cast<unsigned long>(file.tellg());
        uint64_t latest_offset = this->GetKeyOffset(Key);
        std::string result(vlen,'\0');
        file.read(&result[0],vlen);
        if (latest_offset == val_offset) 
        {
            //this value is latest
            this->put(Key,result);
        }

        collected_space += 15+vlen;
        
    }
    saveToVlogSST();
    tail += collected_space;
    utils::de_alloc_file(vlog_filename,origin_tail,collected_space);
}