#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <math.h>

namespace BACH
{
	using label_t = uint16_t;
	using vertex_t = uint32_t;
	using vertex_num_t = uint32_t;
	using vertex_pair_t = uint64_t;
	using edge_t = uint32_t;
	using edge_num_t = uint32_t;
	using edge_len_t = uint32_t;
	using vertex_edge_pair_t = uint64_t;
	using edge_property_t = double_t;
	using idx_t = uint32_t;
	using time_t = uint64_t;
	using filter_t = uint64_t;
	const unsigned long singel_filter_allocation_size = sizeof(size_t) + sizeof(idx_t);
	const unsigned long singel_edge_total_info_size = sizeof(vertex_t) + sizeof(edge_property_t);
	const idx_t NONEINDEX = UINT32_MAX;
	const vertex_t MAXVERTEX = UINT32_MAX;
	//constexpr static auto TIMEOUT = std::chrono::milliseconds(1);
	const time_t MAXTIME = UINT64_MAX;
	const edge_property_t TOMBSTONE = std::numeric_limits<double>::max();
}