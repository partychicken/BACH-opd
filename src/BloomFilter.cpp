#include "BACH/sstable/BloomFilter.h"

namespace BACH {
    BloomFilter::BloomFilter(idx_t keys_num, double false_positive) {
        if (keys_num == 0) {
            bits_per_key = 0;
            return;
        }
        int32_t bits_num = -1 * static_cast<int32_t>(std::log(false_positive) * keys_num / 0.4804530139182014);
        bits_array.resize((bits_num + 7) / 8);
        bits_num = static_cast<int>(bits_array.size()) * 8; // ע��˴�
        bits_per_key = bits_num / keys_num;
        // ������ѵĹ�ϣ��������
        hash_func_num = static_cast<int32_t>(0.6931471805599453 * bits_per_key);
        // ��֤��ϣ������[1,32]�ķ�Χ�ڣ���ֹ������߹�С
        if (hash_func_num < 1) {
            hash_func_num = 1;
        }
        if (hash_func_num > 32) {
            hash_func_num = 32;
        }
    }

    void BloomFilter::insert(const vertex_t &dst) {
        uint32_t bits_size = bits_array.size() * 8; // λ����ĳ���
        std::string s;
        util::PutFixed(s, dst);
        uint32_t h = util::murmur_hash2(s.c_str(), s.size());
        uint32_t delta = (h >> 17) | (h << 15); // ��17λ�͵�15λ����
        // ģ�����hash_func_num�ι�ϣ
        for (int j = 0; j < hash_func_num; ++j) {
            uint32_t bit_pos = h % bits_size;
            bits_array[bit_pos / 8] |= (1 << (bit_pos % 8));
            // ÿ��ѭ��h������һ��delta�����൱��ÿһ�ֶ�������һ��hash
            h += delta;
        }
    }

    void BloomFilter::insert(const std::string &s) {
        uint32_t bits_size = bits_array.size() * 8; // λ����ĳ���
        uint32_t h = util::murmur_hash2(s.c_str(), s.size());
        uint32_t delta = (h >> 17) | (h << 15); // ��17λ�͵�15λ����
        // ģ�����hash_func_num�ι�ϣ
        for (int j = 0; j < hash_func_num; ++j) {
            uint32_t bit_pos = h % bits_size;
            bits_array[bit_pos / 8] |= (1 << (bit_pos % 8));
            // ÿ��ѭ��h������һ��delta�����൱��ÿһ�ֶ�������һ��hash
            h += delta;
        }
    }

    bool BloomFilter::exists(const vertex_t &dst) {
        uint32_t bits_size = bits_array.size() * 8; // λ����ĳ���
        std::string s;
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

    bool BloomFilter::exists(const std::string &s) {
        uint32_t bits_size = bits_array.size() * 8; // λ����ĳ���
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

    void BloomFilter::create_from_data(int32_t func_num, std::string &bits) {
        hash_func_num = func_num;
        bits_array = bits;
    }

    std::string &BloomFilter::data() {
        return bits_array;
    }
}
