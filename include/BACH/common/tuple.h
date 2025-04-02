#pragma once
#include "BACH/utils/ConcurrentArray.h"
#include <string>

namespace BACH {
    struct Tuple {


        int col_num = 0;
        std::vector <std::string> row;

        std::string GetRow(const int &col) {
            return row[col];
        }
    };
}
