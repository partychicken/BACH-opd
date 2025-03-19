#pragma once
#include "BACH/utils/ConcurrentArray.h"
#include <string>

namespace BACH {
    struct Tuple {
        int col_num;
        std::vector <std::string> row;

        std::string GetRow(int col) {
            return row[col];
        }
    };
}
