#include <atomic>
#include <map>
#include <unordered_map>
#include <tbb/concurrent_vector.h>
#include "BACH/utils/utils.h"

#pragma once

namespace BACH
{
	struct VertexLabelIdEntry
	{
		label_t label;
		tbb::concurrent_vector <std::string> VertexProperty;
		std::map <time_t, vertex_t> VertexCntTable;
		tbb::concurrent_vector <vertex_t>FileIndex;
		std::atomic <vertex_t> total_vertex;
		vertex_t unpersistence = 0;
		size_t property_size = 0;
		idx_t property_file_cnt = 0;
		std::unordered_map <vertex_t, time_t> deletetime;
		std::shared_mutex mutex;
		VertexLabelIdEntry(label_t label_id) :
			label(label_id), total_vertex(0), mutex() {}
	};
}