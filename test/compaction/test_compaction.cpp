#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include "catch.hpp"
#include <gperftools/profiler.h>

std::string value_set[] = {"atarashi", "furui", "ckai", "shiroi", "ccc", "ddd", "eee", "fff", "ggg", "hhh"};


using namespace BACH;

TEST_CASE("FILE IO Test", "[compaction]") {
    for (auto &i: value_set) {
        i.resize(128, ' ');
    }
    ProfilerStart("fileIOTest.prof");

    std::shared_ptr<Options> MockOptions = std::make_shared<Options>();
    MockOptions->MEM_TABLE_MAX_SIZE = 4 * 1024;
    MockOptions->READ_BUFFER_SIZE = 64 * 1024;
    MockOptions->WRITE_BUFFER_SIZE = 64 * 1024;
    MockOptions->MAX_BLOCK_SIZE = 4 * 1024;

    DB x(MockOptions, 2);

    std::vector<std::string> ans_sheet;
    for (int i = 0; i < 1024 * 32 - 5; i++) {
        Tuple t;
        int k = i % 9;
        std::string tmp = std::to_string(i);
        tmp.resize(16);
        t.row.push_back(tmp);
        t.row.push_back(value_set[k]);

        //{
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //    t.row.push_back(value_set[k]);
        //}


        {auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0));
        }
        {auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0));
        }
        ans_sheet.push_back(value_set[k]);
    }

    //for (int i = 0; i < 1024 * 32; i++) {
    //    Tuple t;
    //    int k = rand() & 3;
    //    t.row.push_back(std::to_string(i));
    //    t.row.push_back(value_set[k]);
    //    auto y = x.BeginRelTransaction();
    //    y.PutTuple(t, t.GetRow(0), 1.0);
    //    ans_sheet.push_back(value_set[k]);
    //}
    //sleep(5);
    //while (x.Compacting()) {
    //	std::cout << "Compacting..." << std::endl;
    //	std::this_thread::sleep_for(std::chrono::milliseconds(100));
    //}
    std::cout << "\nFinished insert" << std::endl;
    auto k = x.BeginRelTransaction();
    auto z = x.BeginReadOnlyRelTransaction();
    for (int i = 0; i < 1024 * 32 - 5; i++) {
        std::string tmp = std::to_string(i);
        tmp.resize(16);
        auto t = z.GetTuple(tmp);
        // std::cout << t.GetRow(1) << std::endl;
        REQUIRE(std::string(t.GetRow(0).c_str()) == std::to_string(i));
        REQUIRE(std::string(t.GetRow(1).c_str()) == ans_sheet[i]);
        // REQUIRE(std::string(t.GetRow(1).c_str()) != "");
    }
    // std::cout<<k.GetTuple("0").col_num<<std::endl;
    // for (int i = 0; i < 64; i++) {
    //     z.GetTuplesFromRange(1, "a", "b");
    // }
    ProfilerStop();
}
