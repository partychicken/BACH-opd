#pragma once

#include <cmath>
#include <vector>
#include <string>
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"

namespace BACH
{
	class BloomFilter
	{
	public:
		BloomFilter() = default;
		~BloomFilter() = default;
		BloomFilter(idx_t keys_num, double false_positive);
		std::string& data();
		void insert(const vertex_t& dst);
		void insert(const std::string& dst);
		void create_from_data(int32_t func_num, std::string& bits);
		bool exists(const vertex_t& dst);
		bool exists(const std::string& dst);
		int32_t get_func_num()const { return hash_func_num; }
	private:
		int32_t bits_per_key = 0;
		int32_t hash_func_num = 0;
		std::string bits_array;
	};


	// δ��ɣ������ǽ�keyת��Ϊhashֵ���Ӷ�ʹ��ԭ����bloomfilter�����ھ�ֻ�Ƿ���1��true���ȣ�֮����ô�ҲҪ�ĳ���bloomfilter��
	template<typename key_type>
	idx_t transferKeyToHash(key_type key) {
		return 1;
	}
}