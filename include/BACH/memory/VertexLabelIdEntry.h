#include <atomic>
#include <map>
#include <shared_mutex>
#include <unordered_map>
#include "BACH/utils/utils.h"
#include "BACH/utils/ConcurrentArray.h"

#pragma once

namespace BACH
{
	struct VertexLabelEntry
	{
		std::vector <std::string> VertexProperty;
		std::vector <vertex_t> PropertyID;
		ConcurrentArray <vertex_t> FileIndex;
		std::atomic<vertex_t> total_vertex = 0;
		std::atomic<vertex_t> total_property = 0;
		vertex_t unpersistence = 0;
		size_t property_size = 0;
		idx_t property_file_cnt = 0;
		std::unordered_map <vertex_t, time_t> deletetime;
		std::shared_mutex mutex;
		VertexLabelEntry() :
			mutex() {}
	};
}