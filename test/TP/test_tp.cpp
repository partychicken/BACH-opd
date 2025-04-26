#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include "catch.hpp"


std::string value_set[] = { "atarashi", "furui", "akai", "shiroi" };

using namespace BACH;

TEST_CASE("update test", "[TP]") {
    DB x(std::make_shared<Options>(), 2);
    
    std::vector<std::string> ans_sheet(200);
    for (int i = 0; i < 2048; i++) {
        Tuple t;
        t.row.push_back(std::to_string(i % 200));
        t.row.push_back(std::to_string(i));
        auto y = x.BeginRelTransaction();
        y.PutTuple(t, std::to_string(i % 200), 1.0);
		ans_sheet[i % 200] = std::to_string(i);
    }

    std::cout << "Finished insert" << std::endl;

    //TP test
    auto z = x.BeginRelTransaction();
    auto t = z.GetTuple("0");
	for (int i = 0; i < 200; i++) {
		auto t = z.GetTuple(std::to_string(i));
        REQUIRE(t.GetRow(0) == std::to_string(i));
        REQUIRE(t.GetRow(1) == ans_sheet[i]);

	}


    // auto zz = x.BeginRelTransaction();
    // zz.DelTuple(Tuple(), "0");
    // auto zzz = x.BeginRelTransaction();
    // auto bt = z.GetTuple("0");
    // auto at = zzz.GetTuple("0");

    std::cout << t.row.size() << std::endl;
    // std :: cout << bt.row.size() << std::endl;
    // std :: cout << at.row.size() << std::endl;
    // int a;
    // std::cin >> a;
}

