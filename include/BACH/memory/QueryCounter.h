#pragma once

#include <math.h>
#include "BACH/utils/FixedSegmentTree.h"
#include "BACH/utils/FixedDoubleBitList.h"
#include "BACH/utils/types.h"

namespace BACH
{
	class QueryCounter
	{
	public:
		QueryCounter(std::shared_ptr<Options> options) :
			recent_read(options),
			recent_write(options),
			write(options),
			deletion(options),
			list_num(options->QUERY_LIST_SIZE),
			memory_num(options->MEMORY_MERGE_NUM) {}
		~QueryCounter() = default;
		void AddVertex()
		{
			write.push_back();
			deletion.push_back();
			recent_read.push_back();
			recent_write.push_back();
			query_list.push_back(NULL);
			query_list_empty.emplace_back_default();
		}
		void AddRead(vertex_t src)
		{
			add_recent_query(src, 0);
		}
		void AddWrite(vertex_t src)
		{
			write.add_at(src, 1);
			add_recent_query(src, 1);
		}
		void AddDeletion(vertex_t src)
		{
			deletion.add_at(src, 1);
			add_recent_query(src, 1);
		}
		//1: leveling 2: tiering
		idx_t GetCompactionType(vertex_t src_b, idx_t level)
		{
			size_t W = write.range_query(src_b, level);
			size_t D = deletion.range_query(src_b, level);
			size_t rR = recent_write.range_query(src_b, level);
			size_t rW = recent_read.range_query(src_b, level);
			//return 1;
			if (std::sqrt(1.0 * D / W) + (1.0 * rR / (rW + rR)) > 1)
				return 1;
			else
				return 2;
		}

	private:
		ConcurrentArray<std::shared_ptr<FixedDoubleBitList<1>>> query_list;
		ConcurrentArray<std::atomic<bool>> query_list_empty;
		FixedSegmentTree recent_read, recent_write;
		FixedSegmentTree write, deletion;
		size_t list_num;
		vertex_t memory_num;
		//0: read, 1: write
		void add_recent_query(vertex_t src, idx_t type)
		{
			//std::make_shared<FixedDoubleBitList<1>>(list_num)
			auto k = src / memory_num;
			auto ql = query_list[k];
			while (ql == NULL)
			{
				bool bo = false;
				if (query_list_empty[k].compare_exchange_weak(bo, true))
				{
					query_list[k] = std::make_shared<FixedDoubleBitList<1>>(list_num);
				}
				ql = query_list[k];
			}
			switch (ql->push_back(type))
			{
			case 0:
				if (type == 1)
				{
					recent_read.add_at(src, -1);
					recent_write.add_at(src, 1);
				}
				break;
			case 1:
				if (type == 0)
				{
					recent_read.add_at(src, 1);
					recent_write.add_at(src, -1);
				}
				break;
			}
		}
	};
}