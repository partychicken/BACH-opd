#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include "catch.hpp"


std::string value_set[] = {"atarashi", "furui", "akai", "shiroi"};


using namespace BACH;

TEST_CASE("SCANK Test", "[scanK]") {
    std::shared_ptr<Options> MockOptions = std::make_shared<Options>();
    MockOptions->MEM_TABLE_MAX_SIZE = 4 * 1024;
    MockOptions->READ_BUFFER_SIZE = 64 * 1024;
    MockOptions->WRITE_BUFFER_SIZE = 64 * 1024;
    MockOptions->MAX_BLOCK_SIZE = 4 * 1024;

    DB x(MockOptions, 2);

    std::vector<std::string> ans_sheet;
    for (int i = 0; i < 1024 * 32; i++) {
        Tuple t;
        int k = rand() & 3;
        std::string tmp = std::to_string(i);
        tmp.resize(16);
        t.row.push_back(tmp);
        t.row.push_back(value_set[k]);
        auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0), 1.0);
    }

    sleep(5);
    std::cout << "\nFinished insert" << std::endl;
    auto z = x.BeginRelTransaction();
    auto res = z.ScanKTuples(50, "1");
    for (auto i : res) {
        std::cout << i.GetKey() << std::endl;
    }
    std::cout << std::endl << "======================" << std::endl;
    auto z2 = x.BeginRelTransaction();
    auto res2 = z.ScanKTuples(50, "3");
    for (auto i: res2) {
        std::cout << i.GetKey() << std::endl;
    }
}
