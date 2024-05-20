#include <iostream>
#include <cstdint>
#include <string>
#include <assert.h>
#include <chrono>
#include "test.h"

class TimeRecord {
	std::chrono::steady_clock::time_point time_begin;
public:
	TimeRecord() {
		time_begin = std::chrono::steady_clock::now();
	}

	float get_elapsed_time_micro() {
		std::chrono::steady_clock::time_point time_end = std::chrono::steady_clock::now();
		return (std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_begin).count());
	}

	void reset() {
		time_begin = std::chrono::steady_clock::now();
	}

};

class CorrectnessTest : public Test
{
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
	const uint64_t LARGE_TEST_MAX = 1024 * 64;
	const uint64_t GC_TEST_MAX = 1024 * 48;

	void regular_test()
	{
		uint64_t i;
		TimeRecord record;
		std::vector<float> put_time_vec;
		std::vector<float> get_time_vec;
		std::vector<float> del_time_vec;
		std::vector<float> scan_time_vec;
		put_time_vec.reserve(1025);
		get_time_vec.reserve(1025);
		del_time_vec.reserve(1025);
		scan_time_vec.reserve(1025);
		// Test multiple key-value pairs
		for (i = 0; i < 1024; ++i)
		{
			record.reset();
			store.put(i, std::string(i + 1, 's'));
			put_time_vec.push_back(record.get_elapsed_time_micro());
		}
		for (int i = 0; i < 1024; i++)
		{
			record.reset();
			store.get(i);
			get_time_vec.push_back(record.get_elapsed_time_micro());
		}
		for (size_t i = 0; i < 1024; i++)
		{
			record.reset();
			store.del(i);
			del_time_vec.push_back(record.get_elapsed_time_micro());
		}
		std::list<std::pair<uint64_t, std::string>> list;
		for (int i = 0; i < 1024; i++)
		{
            list.clear();
			record.reset();
			store.scan(i,i+10,list);
			scan_time_vec.push_back(record.get_elapsed_time_micro());
		}
		double all_time = 0;
        double average_time = 0;
        for (auto single_time:put_time_vec)
        {
            all_time += single_time;
            
        }
        average_time = all_time / 1024;
        std::cout<< "average PUT operation time "<<average_time<<std::endl;
        std::cout<< "throughput of PUT operation is "<<1000000/average_time <<std::endl;
        all_time = 0;
        average_time = 0;
        for (auto single_time:get_time_vec)
        {
            all_time += single_time;
        }
        average_time = all_time / 1024;
        std::cout<< "average GET operation time "<<average_time<<std::endl;
        std::cout<< "throughput of GET operation is "<<1000000/average_time <<std::endl;

        all_time = 0;
        average_time = 0;
        for (auto single_time:del_time_vec)
        {
            all_time += single_time;
        }
        average_time = all_time / 1024;
        std::cout<< "average DEL operation time "<<average_time<<std::endl;
        std::cout<< "throughput of DEL operation is "<<1000000/average_time <<std::endl;

        all_time = 0;
        average_time = 0;
        for (auto single_time:scan_time_vec)
        {
            all_time += single_time;
        }
        average_time = all_time / 1024;
        std::cout<< "average SCAN operation time "<<average_time<<std::endl;
        std::cout<< "throughput of SCAN operation is "<<1000000/average_time <<std::endl;

		
	}

public:
	CorrectnessTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Efficience Test" << std::endl;

		store.reset();

		std::cout << "[Normal test]" << std::endl;
		regular_test();
	}
};

int main(int argc, char *argv[])
{

	CorrectnessTest test("./data", "./data/vlog");

	test.start_test();

	return 0;
}
