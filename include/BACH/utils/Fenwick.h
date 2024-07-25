#pragma once 

#include "ConcurrentArray.h"

namespace BACH
{
	template <typename T>
	class Fenwick
	{
	public:
		Fenwick()
		{
			tree.push_back(T());
		}
		~Fenwick() = default;
		void add_at(size_t i, T delta)
		{
			while (i < tree.size())
			{
				tree[i] += delta;
				i += lowbit(i);
			}
		}
		void push_back(T x)
		{
			size_t end = tree.size() - lowbit(tree.size());
			size_t pos = tree.size() - 1;
			while (pos > end)
			{
				x += tree[pos];
				pos -= lowbit(pos);
			}
			tree.push_back(x);
		}
		T query(size_t i)
		{
			T sum = 0;
			while (i > 0)
			{
				sum += tree[i];
				i -= lowbit(i);
			}
			return sum;
		}
		T range_query(size_t l, size_t r)
		{
			if (r >= tree.size())
				r = tree.size() - 1;
			return query(r) - query(l - 1);
		}

	private:
		ConcurrentArray<T> tree;
		size_t lowbit(size_t x)
		{
			return x & (-x);
		}
	};
}