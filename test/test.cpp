#include <iostream>
#include <BACH/BACH.h>
using namespace BACH;

std::string value_set[] = {"atarashi", "furui", "akai", "shiroi"};

int main() {
    DB x(std::make_shared<Options>(), 2);
    auto y = x.BeginRelTransaction();
    for (int i = 0; i < 1000; i++) {
        Tuple t;
        t.row.push_back(std::to_string(i));
        t.row.push_back(value_set[rand() & 3]);
        y.PutTuple(t, std::to_string(i), 1.0);
    }
    int a;
    std::cin >> a;
    return 0;
}
