#pragma once
#include "BACH/utils/ConcurrentArray.h"
#include <string>
#include "BACH/utils/types.h"


namespace BACH {

    typedef std::string tp_key;

    struct Tuple {

		Tuple() : col_num(0), property(NONEINDEX) {}

        int col_num = 0;
        std::vector <std::string> row;

        tuple_property_t property;

        std::string GetRow(const int &col) {
            return row[col];
        }
    };
}
