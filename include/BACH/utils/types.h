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
	using vertex_pair_t = uint64_t;
	using edge_t = uint32_t;
	using vertex_edge_pair_t = uint64_t;
	using edge_property_t = double_t;
	using idx_t = uint32_t;
	using time_t = uint64_t;
	const idx_t NONEINDEX = UINT32_MAX;
	const vertex_t MAXVERTEX = UINT32_MAX;
	//constexpr static auto TIMEOUT = std::chrono::milliseconds(1);
	const time_t MAXTIME = UINT64_MAX;
	const edge_property_t TOMBSTONE = std::numeric_limits<double>::max();
}