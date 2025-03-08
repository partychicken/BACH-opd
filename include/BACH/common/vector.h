#pragma once

#include <sul/dynamic_bitset.hpp>

namespace BACH {
    const STANDARD_VECTOR_SIZE = 1024;
    class Vector {
    public:
        int Slice(const Vector &other) {
            bitmap &= other.bitmap;
            return bitmap.count();
        }

        int Slice(const Vector &other) {
            bitmap &= other.bitmap;
        }

    private:
        sul::dynamic_bitset<> bitmap(STANDARD_VECTOR_SIZE);
    }
}
