#include <iostream>
#include <BACH/BACH.h>
using namespace BACH;

std::string value_set[] = {"atarashi", "furui", "akai", "shiroi"};



int main() {
    DB x(std::make_shared<Options>(), 2);
    auto y = x.BeginRelTransaction();
    for (int i = 0; i < 2000000; i++) {
        Tuple t;
        t.row.push_back(std::to_string(i));
        t.row.push_back(value_set[rand() & 3]);
        y.PutTuple(t, std::to_string(i), 1.0);
    }

    std :: cout << "Finished insert" << std::endl;

    //TP test
    auto z = x.BeginRelTransaction();
    auto t = z.GetTuple("0");
    // auto zz = x.BeginRelTransaction();
    // zz.DelTuple(Tuple(), "0");
    // auto zzz = x.BeginRelTransaction();
    // auto bt = z.GetTuple("0");
    // auto at = zzz.GetTuple("0");

    std :: cout << t.row.size() << std::endl;
    // std :: cout << bt.row.size() << std::endl;
    // std :: cout << at.row.size() << std::endl;
    // int a;
    // std::cin >> a;
    return 0;
}
