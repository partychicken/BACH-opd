#pragma once

#include <sul/dynamic_bitset.hpp>
#include "BACH/compress/ordered_dictionary.h"
#include "BACH/utils/types.h"

namespace BACH {
    const int STANDARD_VECTOR_SIZE = 2048;

    class Vector {
    public:
        explicit Vector(OrderedDictionary *_dict = nullptr)
          : bitmap(STANDARD_VECTOR_SIZE), dictionary(_dict), data_idx(nullptr), count(0), offset(0){
            bitmap.set();
        }

        unsigned long Slice(const sul::dynamic_bitset<> &sel) {
            bitmap &= sel;
            return bitmap.count();
        }

        void SetData(idx_t *data_begin, idx_t _count, idx_t _offset) {
            data_idx = data_begin;
            this->count = _count;
            this->offset = _offset;
        }

        void SetDict(OrderedDictionary *dict) {
            dictionary = dict;
        }

        idx_t GetCount() const {
            return count;
        }

        OrderedDictionary* GetDict() const {
            return dictionary;
        }

        idx_t* GetData() const{
            return data_idx;
        }

        idx_t GetOffset() const {
            return offset;
        }

        //can only get once
        void GetBitmap(sul::dynamic_bitset<> &res) {
            res = std::move(bitmap);
        }

    private:
        sul::dynamic_bitset<> bitmap;
        OrderedDictionary *dictionary;
        idx_t* data_idx;
        idx_t count;
        idx_t offset;
    };
}
