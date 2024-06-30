#pragma once

#include <algorithm>
#include <vector>
#include <memory>
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
			for (auto i : array)
			{
				delete[] i;
			}
		}
		void newblock(size_t size)
		{
			array.push_back(new T[1 << size]);
		}
		T& operator[](size_t index)
		{
			index += 1;
			size_t num = util::highbit(index);
			if (num >= array.size())
			{
				std::cout << "out of range" << std::endl;
			}
			return array[num][index - (1 << num)];
		}
		const T& operator[](size_t index) const
		{
			index += 1;
			size_t num = util::highbit(index);
			if (num >= array.size())
			{
				std::cout << "out of range" << std::endl;
			}
			return array[num][index - (1 << num)];
		}
		void push_back(const T& x)
		{
			++cnt;
			size_t num = util::highbit(cnt);
			if ((1 << num) == cnt)
			{
				newblock(num);
			}
			array[num][cnt - (1 << num)] = x;
		}
		size_t size() const
		{
			return cnt;
		}
		//return the index of the first element that is not less than x
		//T should support operator<= and operator==
		size_t lowerbound(T x) const
		{
			size_t pf = array.size() - 1;
			while (!(array[pf][0] <= x) && pf > 0)
			{
				--pf;
			}
			if (array[pf][0] == x)
			{
				return (1 << pf) - 1;
			}
			return (1 << pf) + std::lower_bound(array[pf], array[pf] + (1 << pf), x) - array[pf];
		}

	private:
		std::vector<T*> array;
		size_t cnt = 0;
	};
}