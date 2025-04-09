#pragma once

#include <sul/dynamic_bitset.hpp>
#include "BACH/compress/ordered_dictionary.h"
#include "BACH/utils/types.h"

namespace BACH {
    const int STANDARD_VECTOR_SIZE = 2048;

    class Vector {
    public:
        explicit Vector(std::shared_ptr<OrderedDictionary>&&_dict = nullptr)
          : bitmap(STANDARD_VECTOR_SIZE), dictionary(_dict), data_idx(nullptr), count(0){
            bitmap.set();
        }

        unsigned long Slice(const sul::dynamic_bitset<> &sel) {
            bitmap &= sel;
            return bitmap.count();
        }

        void SetData(idx_t *data_begin, idx_t _count) {
            data_idx = data_begin;
            this->count = _count;
        }

        void SetDict(std::shared_ptr<OrderedDictionary>&&dict) {
            dictionary = dict;
        }

    private:
        sul::dynamic_bitset<> bitmap;
        std::shared_ptr<OrderedDictionary>dictionary;
        idx_t* data_idx;
        idx_t count;
    };
}
