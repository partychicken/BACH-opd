#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include "catch.hpp"


std::string value_set[] = {"atarashi", "furui", "akai", "shiroi"};


using namespace BACH;

TEST_CASE("FILE IO Test", "[compaction]") {
    std::shared_ptr<Options> MockOptions = std::make_shared<Options>();
    MockOptions->MEM_TABLE_MAX_SIZE = 4 * 1024;
    MockOptions->READ_BUFFER_SIZE = 64 * 1024;
    MockOptions->WRITE_BUFFER_SIZE = 64 * 1024;
    MockOptions->MAX_BLOCK_SIZE = 64 * 1024;

    DB x(MockOptions, 2);

    std::vector<std::string> ans_sheet;
    for (int i = 0; i < 1024 * 32; i++) {
        Tuple t;
        int k = rand() & 3;
        t.row.push_back(std::to_string(i));
        t.row.push_back(value_set[k]);
        auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0), 1.0);
    }

    for (int i = 0; i < 1024 * 32; i++) {
        Tuple t;
        int k = rand() & 3;
        t.row.push_back(std::to_string(i));
        t.row.push_back(value_set[k]);
        auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0), 1.0);
        ans_sheet.push_back(value_set[k]);
    }
    sleep(5);
    std::cout << "\nFinished insert" << std::endl;
    auto z = x.BeginRelTransaction();
    for (int i = 0; i < 1024 * 32; i++) {
        auto t = z.GetTuple(std::to_string(i));
        // std::cout << t.GetRow(1) << std::endl;
        REQUIRE(std::string(t.GetRow(0).c_str()) == std::to_string(i));
        REQUIRE(std::string(t.GetRow(1).c_str()) == ans_sheet[i]);
        REQUIRE(std::string(t.GetRow(1).c_str()) != "");
    }
}
