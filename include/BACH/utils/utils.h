#pragma once

#include <math.h>
#include <memory>
#include <string>
#include "Options.h"
#include "types.h"
namespace BACH
{
	namespace util
	{
		template<typename T>
		inline void PutFixed(std::string& dst, T val)
		{
			uint32_t n = sizeof(val);
			dst.append((char*)(&val), n);
		}
		template<typename T>
		inline void DecodeFixed(const char* data, T& val)
		{
			val = *(reinterpret_cast<const T*>(data));
		}

		inline uint32_t murmur_hash2(const void* key, uint32_t len) {
			// 'm' and 'r' are mixing constants generated offline.
			// They're not really 'magic', they just happen to work well.
			static constexpr uint32_t seed = 0xbc451d34;
			static constexpr uint32_t m = 0x5bd1e995;
			static constexpr uint32_t r = 24;

			// Initialize the hash to a 'random' value

			uint32_t h = seed ^ len;

			// Mix 4 bytes at a time into the hash

			const uint8_t* data = (const unsigned char*)key;

			while (len >= 4) {
				uint32_t k = *(uint32_t*)data;

				k *= m;
				k ^= k >> r;
				k *= m;

				h *= m;
				h ^= k;

				data += 4;
				len -= 4;
			}

			// Handle the last few bytes of the input array

			switch (len) {
			case 3:
				h ^= data[2] << 16;
			case 2:
				h ^= data[1] << 8;
			case 1:
				h ^= data[0];
				h *= m;
			};

			// Do a few final mixes of the hash to ensure the last few
			// bytes are well-incorporated.

			h ^= h >> 13;
			h *= m;
			h ^= h >> 15;

			return h;
		}

		inline std::string BuildSSTPath(std::string_view label,
			uint32_t level, vertex_t first_vertex, idx_t id)
		{
			return static_cast<std::string>(label) + "_"
				+ std::to_string(level) + "_"
				+ std::to_string(first_vertex) + "_"
				+ std::to_string(id) + ".sst";
		}
		inline idx_t GetFileIdBySSTPath(std::string sst_path)
		{
			idx_t file_id = 0;
			idx_t x = 1;
			for (auto i = sst_path.size(); i > 0; i++)
			{
				if (sst_path[i] == '_')
					break;
				file_id = file_id + x * (sst_path[i] - '0');
				x *= 10;
			}
			return file_id;
		}
		template<typename A, typename B>
		inline A fast_pow(A a, B b)
		{
			A ans = 1;
			for (; b; b >>= 1, a = a * a)
				if (b & 1)
					ans *= a;
			return ans;
		}
		inline vertex_t ClacFileSize(vertex_t MERGE_NUM, idx_t level)
		{
			return fast_pow(MERGE_NUM, level + 1);
		}
		inline std::pair<vertex_t, vertex_t> ClacChunkAndListId(
			vertex_t MERGE_NUM, idx_t level, vertex_t vertex_id)
		{
			vertex_t list_size = util::ClacFileSize(MERGE_NUM, level);
			vertex_t chunk_size = list_size * MERGE_NUM;
			vertex_t chunk_id = vertex_id / chunk_size;
			vertex_t list_id = (vertex_id - chunk_id * chunk_size)
				/ list_size;
			return std::make_pair(chunk_id, list_id);
		}

		inline vertex_pair_t make_vertex_pair(vertex_t src, vertex_t dst)
		{
			return ((vertex_pair_t)src << (sizeof(vertex_t) * 8)) | dst;
		}
		inline vertex_edge_pair_t make_vertex_edge_pair(vertex_t dst, edge_t edge)
		{
			return ((vertex_edge_pair_t)(dst + 1) << (sizeof(vertex_t) * 8)) | edge;
		}
		inline vertex_t unzip_pair_first(vertex_edge_pair_t x)
		{
			return (vertex_t)(x >> (sizeof(vertex_t) * 8)) - 1;
		}
		inline edge_t unzip_pair_second(vertex_edge_pair_t x)
		{
			return (edge_t)(x & (((vertex_edge_pair_t)1 << (sizeof(vertex_t) * 8)) - 1));
		}
		inline size_t highbit(size_t x)
		{
			for (size_t i = 1; i < 64; ++i)
				if (x >> i == 0)
					return i - 1;
			return 64;
		}
		template<typename T>
		inline T GetDecodeFixed(const char* data)
		{
			return *(reinterpret_cast<const T*>(data));
		}
	}
}