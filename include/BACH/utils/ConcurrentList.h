#pragma once

#include <atomic>
#include <vector>

namespace BACH
{
	//template<typename T, bool ref = false>
	class ConcurrentList
	{
	public:
		ConcurrentList(size_t size)
		{
			for (auto i = 0; i < size; i++)
			{
				stack[i] = i;
			}
			top = size;
			data.resize(size);
			prev.resize(size, -1);
			next.resize(size, -1);
			front = back = -1;
		}
		size_t add(int x)
		{
			size_t pos;
			do
			{
				pos=back.load();
			}
		}

		~ConcurrentList();

	private:
		std::vector<size_t> stack;
		std::atomic<size_t> top;
		std::vector<int> data;
		std::vector<size_t> prev;
		std::vector<size_t> next;
		std::atomic<size_t> front, back;
		//if constexpr (ref)
		//{
		//	std::vector<T> refs;
		//}

	};

	ConcurrentList::ConcurrentList()
	{
	}

	ConcurrentList::~ConcurrentList()
	{
	}
}