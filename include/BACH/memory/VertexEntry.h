#pragma once
//#ifndef VERTEXENTRY
//#define VERTEXENTRY

#include <atomic>
#include <memory>
#include <vector>
#include <PMA/CPMA.hpp>
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
		std::vector<std::shared_ptr<VertexEntry>> entry;
		std::shared_ptr<SizeEntry> last;
		std::atomic<bool> immutable;
		time_t deletion_time = MAXTIME;
		SizeEntry(vertex_t _begin_vertex_id, std::shared_ptr<SizeEntry>_last) :
			begin_vertex_id(_begin_vertex_id), last(_last), immutable(false)
		{}
	};
	struct VertexEntry
	{
		CPMA<spmae_settings<vertex_edge_pair_t>>EdgeIndex;
		std::vector <EdgeEntry> EdgePool;
		std::shared_ptr <SizeEntry> size_info;
		std::shared_mutex mutex;
		time_t deadtime = MAXTIME;
		std::shared_ptr<VertexEntry> next;
		VertexEntry(std::shared_ptr<SizeEntry> size_info,
			std::shared_ptr<VertexEntry> _next = NULL) :
			mutex(), next(_next)
		{
			this->size_info = size_info;
			size_info->entry.push_back(std::shared_ptr<VertexEntry>(this));
		}
	};
	struct EdgeLabelEntry
	{
		ConcurrentArray<std::shared_ptr<VertexEntry>> VertexIndex;
		std::shared_ptr<SizeEntry> now_size_info;
		label_t src_label_id;
		ConcurrentArray<std::shared_mutex> vertex_mutex;
		std::shared_mutex mutex;
		EdgeLabelEntry(label_t src_label_id) :
			src_label_id(src_label_id),
			now_size_info(std::make_shared<SizeEntry>(0, NULL)),
			mutex() {}
	};
}
//#endif // !VERTEXENTRY