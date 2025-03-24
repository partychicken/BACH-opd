#pragma once

#include <sul/dynamic_bitset.hpp>
#include "dictionary.h"

namespace BACH {
    const int STANDARD_VECTOR_SIZE = 1024;

    template <typename Key_t>
    class Vector {
    public:
        Vector(std::shared_ptr<Dict<Key_t> >_dict = nullptr)
          : bitmap(STANDARD_VECTOR_SIZE), dictionary(_dict){}
        int Slice(const sul::dynamic_bitset<> &sel) {
            bitmap &= sel;
            return bitmap.count();
        }

    private:
        sul::dynamic_bitset<> bitmap;
        std::shared_ptr<Dict<Key_t> >dictionary;
        idx_t* data_idx;
    };
}
