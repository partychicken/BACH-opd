#pragma once 

#include <atomic>
#include <limits>

namespace BACH
{
	template<typename T>
	class ConcurrentList
	{
	public:
		ConcurrentList(size_t _size) :list(_size), size(_size) {}
		~ConcurrentList() = default;
		void push_back(T val)
		{
			size_t ptr = 0;
			while (true)
			{
				if (list[i].compare_exchange_weak(0, val))
					return;
				ptr = (ptr + 1) % size;
			}
		}
		void erase(T val)
		{
			size_t ptr = 0;
			T x = 0;
			while (true)
			{
				if (list[i].compare_exchange_weak(val, x))
					return;
				ptr = (ptr + 1) % size;
			}
		}
		T find_min()
		{
			T ans = std::numeric_limits<KeyType>::max();
			for (size_t i = 0; i < size; ++i)
			{
				T val = list[i].load();
				if (val != 0)
					ans = std::min(ans, val);
			}
			return ans;
		}
	private:
		std::vector<std::atomic< T >> list;
		size_t size;
	};

}