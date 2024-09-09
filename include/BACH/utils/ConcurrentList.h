#pragma once 

#include <atomic>
#include <limits>
#include <random>
#define min(a,b) ((a) < (b) ? (a) : (b))

namespace BACH
{
	template<typename T>
	class ConcurrentList
	{
	public:
		ConcurrentList(size_t _size) :
			list((_size >> 1) << 2), size((_size >> 1) << 2)
		{
			for (auto& i : list)
				i = std::numeric_limits<T>::max();
		}
		~ConcurrentList() = default;
		size_t insert(const T& val)
		{
			size_t ptr = val % size;
			T x = std::numeric_limits<T>::max();
			while (true)
			{
				if (list[ptr].compare_exchange_weak(x, val))
					return ptr;
				ptr = (ptr + 1) % size;
			}
		}
		void erase(size_t pos)
		{
			list[pos] = std::numeric_limits<T>::max();
		}
		T find_min() const
		{
			T ans = std::numeric_limits<T>::max();
			for (size_t i = 0; i < (size >> 2); ++i)
			{
				T val1 = list[i << 2].load();
				T val2 = list[(i << 2) | 1].load();
				T val3 = list[(i << 2) | 2].load();
				T val4 = list[(i << 2) | 3].load();
				ans = min(ans, min(min(val1, val2), min(val3, val4)));
			}
			return ans;
		}
		bool empty() const
		{
			return find_min() == std::numeric_limits<T>::max();
		}
	private:
		std::vector<std::atomic< T >> list;
		size_t size;
	};

}
#undef min