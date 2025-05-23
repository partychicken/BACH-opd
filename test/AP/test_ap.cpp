#define CATCH_CONFIG_MAIN 

#include "BACH/BACH.h"
#include <iostream>
#include <vector>
#include <string>
#include "catch.hpp"


std::string value_set[] = { "atarashi", "furui", "akai", "shiroi" };



using namespace BACH;


TEST_CASE("FILE VALUE SCAN TEST", "[AP]") {
    {
        std::shared_ptr<Options> MockOptions = std::make_shared<Options>();
        // MockOptions->MEM_TABLE_MAX_SIZE = 4 * 1024;
        // MockOptions->READ_BUFFER_SIZE = 64 * 1024;
        // MockOptions->WRITE_BUFFER_SIZE = 64 * 1024;
        // MockOptions->MAX_BLOCK_SIZE = 64 * 1024;

        DB x(MockOptions, 2);

        //std::vector<std::string> ans_sheet(200);
        for (int i = 0; i < 1024 * 8 * 1024; i++) {
            Tuple t;
            t.row.push_back(std::to_string(i));
            t.row.push_back(std::to_string(i));
            auto y = x.BeginRelTransaction();
            y.PutTuple(t, t.GetRow(0));
            //ans_sheet[i] = std::to_string(i);
        }

        std::cout << "\nFinished insert" << std::endl;

        while (x.Compacting()) { sleep(1); }
        auto z = x.BeginRelTransaction();
        std::string a("10"), b("200");
        z.GetTuplesFromRange(1, a, b);

    }
}
