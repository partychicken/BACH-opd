#include "BACH/sstable/BloomFilter.h"

namespace BACH
{
	BloomFilter::BloomFilter(idx_t keys_num, double false_positive)
	{
		int32_t bits_num = -1 * static_cast<int32_t>(std::log(false_positive) * keys_num / 0.4804530139182014);
		bits_array.resize((bits_num + 7) / 8);
		bits_num = static_cast<int>(bits_array.size()) * 8; // 注意此处
		bits_per_key = bits_num / keys_num;
		// 计算最佳的哈希函数数量
		hash_func_num = static_cast<int32_t>(0.6931471805599453 * bits_per_key);
		// 保证哈希函数在[1,32]的范围内，防止过大或者过小
		if (hash_func_num < 1) {
			hash_func_num = 1;
		}
		if (hash_func_num > 32) {
			hash_func_num = 32;
		}
	}
	void BloomFilter::insert_pair(const vertex_t& src, const vertex_t& dst)
	{
		uint32_t bits_size = bits_array.size() * 8; // 位数组的长度
		std::string s;
		util::PutFixed(s, src);
		util::PutFixed(s, dst);
		uint32_t h = util::murmur_hash2(s.c_str(), s.size());
		uint32_t delta = (h >> 17) | (h << 15); // 高17位和低15位交换
		// 模拟计算hash_func_num次哈希
		for (int j = 0; j < hash_func_num; ++j) {
			uint32_t bit_pos = h % bits_size;
			bits_array[bit_pos / 8] |= (1 << (bit_pos % 8));
			// 每轮循环h都加上一个delta，就相当于每一轮都进行了一次hash
			h += delta;
		}
	}
	bool BloomFilter::exists(const vertex_t& src, const vertex_t& dst)
	{
		uint32_t bits_size = bits_array.size() * 8; // 位数组的长度
		std::string s;
		util::PutFixed(s, src);
		util::PutFixed(s, dst);
		uint32_t h = util::murmur_hash2(s.c_str(), s.size());
		uint32_t delta = (h >> 17) | (h << 15);
		for (int j = 0; j < hash_func_num; ++j) {
			uint32_t bit_pos = h % bits_size;
			if ((bits_array[bit_pos / 8] & (1 << (bit_pos % 8))) == 0) {
				return false;
			}
			h += delta;
		}
		return true;
	}

	void BloomFilter::create_from_data(int32_t func_num, std::string& bits)
	{
		hash_func_num = func_num;
		bits_array = bits;
	}

	std::string& BloomFilter::data()
	{
		return bits_array;
	}
}