#pragma once 

#include <atomic>
#include <limits>
#include <random>

namespace BACH
{
	template<typename T>
	class ConcurrentList
	{
	public:
		ConcurrentList(size_t _size) :list(_size), size(_size) {}
		~ConcurrentList() = default;
		void insert(const T& val)
		{
			size_t ptr = std::rand() % size;
			T x = T();
			while (true)
			{
				if (list[ptr].compare_exchange_weak(x, val))
					return;
				ptr = (ptr + 1) % size;
			}
		}
		void erase(T val)
		{
			size_t ptr = std::rand() % size;
			T x = T();
			while (true)
			{
				T tmp = val;
				if (list[ptr].compare_exchange_weak(tmp, x))
					return;
				ptr = (ptr + 1) % size;
			}
		}
		T find_min() const
		{
			T ans = std::numeric_limits<T>::max();
			for (size_t i = 0; i < size; ++i)
			{
				T val = list[i].load();
				if (val != 0)
					ans = std::min(ans, val);
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