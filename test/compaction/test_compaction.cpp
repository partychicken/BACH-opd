#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include "catch.hpp"
#include <gperftools/profiler.h>

std::string value_set[] = {"atarashi", "furui", "akai", "shiroi"};


using namespace BACH;

TEST_CASE("FILE IO Test", "[compaction]") {
    ProfilerStart("fileIOTest.prof");

    std::shared_ptr<Options> MockOptions = std::make_shared<Options>();
    //MockOptions->MEM_TABLE_MAX_SIZE = 4 * 1024;
    //MockOptions->READ_BUFFER_SIZE = 64 * 1024;
    //MockOptions->WRITE_BUFFER_SIZE = 64 * 1024;
    //MockOptions->MAX_BLOCK_SIZE = 64 * 1024;

    DB x(MockOptions, 2);

    std::vector<std::string> ans_sheet;
    for (int i = 0; i < 1024 * 1024 * 8; i++) {
        Tuple t;
        int k = rand() & 3;
        auto ii = std::to_string(i);
        ii.resize(16);
        t.row.push_back(ii);
        t.row.push_back(value_set[k]);
        auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0), 1.0);
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
	//while (x.Compacting()) {
	//	std::cout << "Compacting..." << std::endl;
	//	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	//}
    std::cout << "\nFinished insert" << std::endl;
    auto z = x.BeginRelTransaction();
    for (int i = 0; i < 1024 * 32; i++) {
        auto ii = std::to_string(i);
        ii.resize(16);
        auto t = z.GetTuple(ii);
        // std::cout << t.GetRow(1) << std::endl;
        REQUIRE(std::string(t.GetRow(0).c_str()) == std::to_string(i));
        //REQUIRE(std::string(t.GetRow(1).c_str()) == ans_sheet[i]);
        REQUIRE(std::string(t.GetRow(1).c_str()) != "");
    }
    z.GetTuplesFromRange(1, "a", "b");
    ProfilerStop();
}
