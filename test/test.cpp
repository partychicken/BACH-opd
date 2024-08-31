#include <iostream>
#include <BACH/BACH.h>
using namespace BACH;
int main()
{
	DB x(std::make_shared<Options>());
	x.AddVertexLabel("a");
	x.AddEdgeLabel("e", "a", "a");
	auto y=x.BeginTransaction();
	for(int i=0;i<=10000000;++i)
		y.AddVertex(0,"");
	int a;
	std::cin >> a;
	return 0;
}