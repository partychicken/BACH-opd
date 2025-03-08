#pragma once
#include "BACH/utils/ConcurrentArray.h"
#include <string>

namespace BACH {
    struct tuple {
        int col_num;
        ConcurrentArray <std::string> row;

        std::string GetRow(int col) {
            return row[col];
        }
    };
}
