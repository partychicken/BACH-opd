#pragma once
//#ifndef VERTEXENTRY
//#define VERTEXENTRY

#include <atomic>
#include <map>
#include <memory>
#include <shared_mutex>
#include <vector>
//#include <PMA/CPMA.hpp>
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
		std::map<vertex_t, edge_t>EdgeIndex;
		std::vector <EdgeEntry> EdgePool;
		std::shared_ptr < SizeEntry > size_info;
		std::shared_mutex mutex;
		time_t deadtime = MAXTIME;
		std::shared_ptr < VertexEntry > next;
		VertexEntry(std::shared_ptr < SizeEntry > size_info,
			std::shared_ptr < VertexEntry > _next = NULL);
	};
	struct SizeEntry
	{
		vertex_t begin_vertex_id;
		size_t size = 0;
		std::vector < std::shared_ptr < VertexEntry >> entry;
		std::shared_ptr < SizeEntry >  last = NULL;
		std::atomic<bool> immutable;
		time_t max_time = 0;
		SizeEntry(vertex_t _begin_k, vertex_t _size);
		void delete_entry();
	};
	struct EdgeLabelEntry
	{
		//ConcurrentArray<std::shared_ptr < VertexEntry >> VertexIndex;
		ConcurrentArray<std::shared_ptr < SizeEntry >> SizeIndex;
		QueryCounter query_counter;
		label_t src_label_id;
		ConcurrentArray<std::shared_mutex> size_mutex;
		std::shared_mutex mutex;
		EdgeLabelEntry(std::shared_ptr<Options> options,
			label_t src_label_id);
	};
}
//#endif // !VERTEXENTRY