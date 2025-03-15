#pragma once

#include <atomic>
#include <iostream>
#include <algorithm>
#include <vector>
#include <memory>
#include <mutex>
#include <BACH/utils/utils.h>

namespace BACH
{
	// a array support concurrent access
	// binary lifting linked list, support random access by using binary index
	// T should be a small and unchangable struct
	template <typename T>
	class ConcurrentArray
	{
	public:
		ConcurrentArray(const ConcurrentArray&) = delete;
		ConcurrentArray& operator=(const ConcurrentArray&) = delete;
		ConcurrentArray() = default;
		~ConcurrentArray()
		{
			for (int i = 0; i < 30; ++i)
			{
				delete[] array[i];
			}
		}
		T& operator[](size_t index)
		{
			if (index >= cnt)
			{
				std::cout << "out of range" << std::endl;
			}
			index += 1;
			size_t num = util::highbit(index);
			return array[num][index - (1 << num)];
		}
		const T& operator[](size_t index) const
		{
			if (index >= cnt)
			{
				std::cout << "out of range" << std::endl;
			}
			index += 1;
			size_t num = util::highbit(index);
			return array[num][index - (1 << num)];
		}
		size_t push_back(const T& x)
		{
			size_t pos = cnt.fetch_add(1);
			size_t num = util::highbit(pos + 1);
			T* tmp = array[num];
			while (tmp == NULL)
			{
				bool bo = false;
				if (empty[num].compare_exchange_weak(bo, true))
				{
					array[num] = new T[1 << num];
					array_size++;
				}
				tmp = array[num];
			}
			//std::cout << pos << " " << num << " " << pos + 1 - (1 << num) << std::endl;
			array[num][pos + 1 - (1 << num)] = x;
			return pos;
		}
		size_t emplace_back_default()
		{
			size_t pos = cnt.fetch_add(1);
			size_t num = util::highbit(pos + 1);
			T* tmp = array[num];
			while (tmp == NULL)
			{
				bool bo = false;
				if (empty[num].compare_exchange_strong(bo, true))
				{
					array[num] = new T[1 << num];
					array_size++;
				}
				tmp = array[num];
			}
			return pos;
		}
		size_t size() const
		{
			return cnt;
		}
		//return the first element that is not more than x
		//T should support operator <= and operator==
		T rlowerbound(T x) const
		{
			size_t pf = array_size - 1;
			while (!(array[pf][0] <= x) && pf > 0)
			{
				--pf;
			}
			if (array[pf][0] == x)
			{
				return array[pf][0];
			}
			auto it = std::upper_bound(array[pf], array[pf]
				+ ((pf == array_size - 1) ?
					cnt + 1 - ((size_t)1 << util::highbit(cnt))
					: ((size_t)1 << pf)), x);
			if (it != array[pf])
				return *(it - 1);
			else
				return T();
		}

	private:
		T* array[30] = { 0 };
		std::atomic<bool> empty[30];
		size_t array_size = 0;
		std::atomic<size_t> cnt = 0;
	};
}