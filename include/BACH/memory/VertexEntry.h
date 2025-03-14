#pragma once

#include <atomic>
#include <memory>
#include <semaphore>
#include <shared_mutex>
#include <vector>
#include <folly/ConcurrentSkipList.h>
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
	struct ComparePair
	{
		bool operator()(const std::pair<vertex_t, edge_property_t>& a,
			const std::pair<vertex_t, edge_property_t>& b) const
		{
			return a.first < b.first;
		}
	};
	typedef folly::ConcurrentSkipList<std::pair<vertex_t, idx_t>, ComparePair> SkipList;
	struct SizeEntry
	{
		vertex_t begin_vertex_id;
		std::vector <std::shared_ptr< SkipList >> edge_index;
		std::vector<std::shared_mutex> mutex;
		ConcurrentArray<EdgeEntry> edge_pool;
		std::shared_ptr<SizeEntry> last = NULL, next = NULL;
		std::atomic<bool> immutable;
		std::counting_semaphore<1024> sema;
		std::map <vertex_t, time_t> del_table;
		size_t size = 0;
		time_t max_time = 0;
		SizeEntry(vertex_t _begin_k, vertex_t _size, 
			std::shared_ptr<SizeEntry> _next = NULL);
		void delete_entry();
	};
	struct EdgeLabelEntry
	{
		//ConcurrentArray<std::shared_ptr < VertexEntry >> VertexIndex;
		ConcurrentArray <std::shared_ptr<SizeEntry>> SizeIndex;
		ConcurrentArray <std::atomic<bool>> size_index_empty;
		QueryCounter query_counter;
		label_t src_label_id;
		std::shared_mutex mutex;
		EdgeLabelEntry(std::shared_ptr<Options> options,
			label_t src_label_id);
	};
}