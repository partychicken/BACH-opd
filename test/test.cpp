#include <iostream>
#include <BACH/BACH.h>
int main()
{
	std::cout << BACH::util::highbit(10) << std::endl;
	std::cout << BACH::util::highbit(100) << std::endl;
	std::cout << BACH::util::highbit(1000) << std::endl;
	return 0;
}