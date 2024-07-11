#pragma once

#include <cstring>

namespace BACH
{
	template <size_t T>
	class FixedDoubleBitList
	{
	public:
		FixedDoubleBitList(size_t len) : max_size(len)
		{
			data = new char[(len + per - 1) / per];
			memset(data, 0, (len + per - 1) / per);
		}
		~FixedDoubleBitList()
		{
			delete data;
		}
		char push_back(char x)
		{
			char ans = 0;
			if (size == max_size)
			{
				ans = data[head / per] & (mask << ((head % per) * T));
				data[head / per] ^= ans;
				ans >>= (head % per) * T;
			}
			else
			{
				++size;
			}
			data[head / per] |= x << ((head % per) * T);
			head = (head + 1) % max_size;
			return ans;
		}

	private:
		size_t size = 0, head = 0, max_size;
		constexpr static size_t per = sizeof(char) * 8 / T;
		constexpr static size_t mask = (1 << T) - 1;
		char* data;
	};
}