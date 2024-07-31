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
	struct SizeEntry;
	struct VertexEntry
	{
		CPMA<spmae_settings<vertex_edge_pair_t>>EdgeIndex;
		std::vector <EdgeEntry> EdgePool;
		SizeEntry* size_info;
		std::shared_mutex mutex;
		time_t deadtime = MAXTIME;
		VertexEntry* next;
		VertexEntry(SizeEntry* size_info, VertexEntry* _next = NULL);
	};
	struct SizeEntry
	{
		vertex_t begin_vertex_id;
		size_t size = 0;
		std::vector<VertexEntry*> entry;
		SizeEntry* last = NULL;
		std::atomic<bool> immutable;
		time_t max_time = 0;
		SizeEntry(vertex_t _begin_vertex_id, SizeEntry* next);
		void delete_entry();
	};
	struct EdgeLabelEntry
	{
		ConcurrentArray<VertexEntry*> VertexIndex;
		QueryCounter query_counter;
		SizeEntry* now_size_info;
		label_t src_label_id;
		ConcurrentArray<std::shared_mutex> vertex_mutex;
		std::shared_mutex mutex;
		EdgeLabelEntry(label_t src_label_id, size_t list_num);
	};
}
//#endif // !VERTEXENTRY