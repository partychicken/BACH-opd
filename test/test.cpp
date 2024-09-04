#include <iostream>
#include <BACH/BACH.h>
using namespace BACH;
int main()
{
	DB x(std::make_shared<Options>());
	x.AddVertexLabel("a");
	x.AddEdgeLabel("e", "a", "a");
	auto y=x.BeginTransaction();
	std::string aaaa;
	for (int i = 1; i <= 100; ++i)aaaa.push_back('a');
	for(int i=0;i<=10000000;++i)
		y.AddVertex(0,aaaa);
	int a;
	std::cin >> a;
	return 0;
}