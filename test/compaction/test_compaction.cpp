#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include "catch.hpp"


std::string value_set[] = { "atarashi", "furui", "akai", "shiroi" };



using namespace BACH;

TEST_CASE("FILE IO Test", "[compaction]") {
    std::shared_ptr<Options> MockOptions = std::make_shared<Options>();
    MockOptions->MEM_TABLE_MAX_SIZE = 4 * 1024;
    MockOptions->READ_BUFFER_SIZE = 64 * 1024;
    MockOptions->WRITE_BUFFER_SIZE = 64 * 1024;
    MockOptions->MAX_BLOCK_SIZE = 64 * 1024;

    DB x(MockOptions, 2);
    
    //std::vector<std::string> ans_sheet(200);
    for (int i = 0; i < 1024 * 32; i++) {
        Tuple t;
        t.row.push_back(std::to_string(i));
        t.row.push_back(value_set[rand() & 3]);
        auto y = x.BeginRelTransaction();
        y.PutTuple(t, t.GetRow(0), 1.0);
		//ans_sheet[i] = std::to_string(i);
    }

    std::cout << "\nFinished insert" << std::endl;

    while (x.Compacting.load(std::memory_order_acquire)) { sleep(1); }
    auto z = x.BeginRelTransaction();
    auto t = z.GetTuple("0");
	for (int i = 0; i < 200; i++) {
		auto t = z.GetTuple(std::to_string(i));
        std::cout << t.GetRow(1) << std::endl;
        //REQUIRE(t.GetRow(0) == std::to_string(i));
        //REQUIRE(t.GetRow(1) == ans_sheet[i]);
	}
}