#pragma once
#ifndef VERTEXENTRY
#define VERTEXENTRY

#include <map>
#include <memory>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include <PMA/CPMA.hpp>
#include "BACH/utils/types.h"
#include "EdgeEntry.h"
#include "Futex.hpp"
#include "SizeEntry.h"

namespace BACH
{
	struct VertexEntry
	{
		vertex_t id;
		CPMA<spmae_settings<vertex_edge_pair_t>>EdgeIndex;
		std::vector <EdgeEntry> EdgePool;
		std::vector <vertex_t> DstPool;
		std::shared_ptr<SizeEntry> size_info;
		std::shared_mutex mutex;
		time_t deadtime = TOMBSTONE;
		VertexEntry(vertex_t id, std::shared_ptr<SizeEntry> size_info, vertex_t local_id) :
			mutex()
		{
			this->id = id;
			this->size_info = size_info;
			if (size_info->num == 0)
			{
				size_info->vertex_id_b = local_id;
			}
			size_info->vertex_id_e = local_id;
			size_info->num++;
		}
	};
}
#endif // !VERTEXENTRY