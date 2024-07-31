#pragma once

#include <math.h>
#include "BACH/utils/Fenwick.h"
#include "BACH/utils/FixedDoubleBitList.h"
#include "BACH/utils/types.h"

namespace BACH
{
	class QueryCounter
	{
	public:
		QueryCounter(size_t _list_num) :
			list_num(_list_num) {}
		~QueryCounter() = default;
		void AddVertex()
		{
			write.push_back(0);
			deletion.push_back(0);
			recent_read.push_back(0);
			recent_write.push_back(0);
			query_list.push_back(
				std::make_shared<FixedDoubleBitList<1>>(list_num));
		}
		void AddRead(vertex_t src)
		{
			add_recent_query(src, 0);
		}
		void AddWrite(vertex_t src)
		{
			write.add_at(src + 1, 1);
			add_recent_query(src, 1);
		}
		void AddDeletion(vertex_t src)
		{
			deletion.add_at(src + 1, 1);
			add_recent_query(src, 1);
		}
		//1: leveling 2:tiering
		idx_t GetCompactionType(vertex_t src_b, vertex_t src_e)
		{
			size_t W = write.range_query(src_b + 1, src_e + 1);
			size_t D = deletion.range_query(src_b + 1, src_e + 1);
			size_t rR = recent_write.range_query(src_b + 1, src_e + 1);
			size_t rW = recent_read.range_query(src_b + 1, src_e + 1);
			if (std::sqrt((1.0 * D / W) * (1.0 * rR / rW)) > 0.5)
				return 1;
			else
				return 2;
		}

	private:
		ConcurrentArray<std::shared_ptr<FixedDoubleBitList<1>>> query_list;
		Fenwick<size_t> recent_read, recent_write;
		Fenwick<size_t> write, deletion;
		size_t list_num;
		//0: read, 1: write
		void add_recent_query(vertex_t src, idx_t type)
		{
			switch (query_list[src]->push_back(type))
			{
			case 0:
				if (type == 1)
				{
					recent_read.add_at(src + 1, -1);
					recent_write.add_at(src + 1, 1);
				}
				break;
			case 1:
				if (type == 0)
				{
					recent_read.add_at(src + 1, 1);
					recent_write.add_at(src + 1, -1);
				}
				break;
			}
		}
	};
}