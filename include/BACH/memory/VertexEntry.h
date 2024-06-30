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
		size_t size = 0;
		std::vector<std::shared_ptr<VertexEntry>> entry;
		//std::atomic<bool> merging;
	};
	struct VertexEntry
	{
		//vertex_t id;
		CPMA<spmae_settings<vertex_edge_pair_t>>EdgeIndex;
		std::vector <EdgeEntry> EdgePool;
		std::shared_ptr <SizeEntry> size_info;
		std::shared_mutex mutex;
		time_t deadtime = TOMBSTONE;
		VertexEntry(/*vertex_t id, */std::shared_ptr<SizeEntry> size_info) :
			mutex()
		{
			//this->id = id;
			this->size_info = size_info;
			size_info->entry.push_back(std::shared_ptr<VertexEntry>(this));
		}
	};
	struct EdgeLabelEntry
	{
		ConcurrentArray<std::shared_ptr<VertexEntry>> VertexIndex;
		std::shared_ptr<SizeEntry> now_size_info;
		label_t src_label_id;
		std::shared_mutex mutex;
		EdgeLabelEntry(label_t src_label_id) :
			src_label_id(src_label_id),
			now_size_info(std::make_shared<SizeEntry>()),
			mutex() {}
	};
}
//#endif // !VERTEXENTRY