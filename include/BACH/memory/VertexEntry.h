#pragma once
//#ifndef VERTEXENTRY
//#define VERTEXENTRY

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <vector>
#include <PMA/CPMA.hpp>
#include "QueryCounter.h"
#include "BACH/utils/types.h"
#include "BACH/utils/ConcurrentArray.h"

namespace BACH
{
	struct EdgeEntry
	{
		vertex_t dst;
		edge_property_t property;
		time_t time;
		edge_t last_version;
	};
	struct VertexEntry;
	struct SizeEntry
	{
		vertex_t begin_vertex_id;
		size_t size = 0;
		std::vector<VertexEntry*> entry;
		SizeEntry* last;
		std::atomic<bool> immutable;
		time_t deletion_time = MAXTIME;
		SizeEntry(vertex_t _begin_vertex_id, SizeEntry* _last) :
			begin_vertex_id(_begin_vertex_id), last(_last), immutable(false)
		{}
	};
	struct VertexEntry
	{
		CPMA<spmae_settings<vertex_edge_pair_t>>EdgeIndex;
		std::vector <EdgeEntry> EdgePool;
		SizeEntry* size_info;
		std::shared_mutex mutex;
		time_t deadtime = MAXTIME;
		VertexEntry* next;
		VertexEntry(SizeEntry* size_info,
			VertexEntry* _next = NULL) :
			mutex(), next(_next)
		{
			this->size_info = size_info;
			size_info->entry.push_back(this);
			if (_next != NULL)
				deadtime = _next->deadtime;
		}
	};
	struct EdgeLabelEntry
	{
		ConcurrentArray<VertexEntry*> VertexIndex;
		QueryCounter query_counter;
		SizeEntry* now_size_info;
		label_t src_label_id;
		ConcurrentArray<std::shared_mutex> vertex_mutex;
		std::shared_mutex mutex;
		EdgeLabelEntry(label_t src_label_id, size_t list_num) :
			query_counter(list_num),
			src_label_id(src_label_id),
			now_size_info(new SizeEntry(0, NULL)),
			mutex() {}
	};
}
//#endif // !VERTEXENTRY