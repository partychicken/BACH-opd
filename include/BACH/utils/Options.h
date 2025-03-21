#pragma once

#include <string>
#include "types.h"

namespace BACH
{
#ifndef OPTION
#define OPTION

	struct Options
	{
		std::string STORAGE_DIR = "./output/db_storage";
		enum class MergingStrategy
		{
			LEVELING,
			TIERING,
			ELASTIC
		}
		MERGING_STRATEGY = MergingStrategy::ELASTIC;
		size_t MEM_TABLE_MAX_SIZE = 1 * 1024 * 1024;
		size_t LEVEL_0_MAX_SIZE = 512 * 1024 * 1024;
		size_t VERTEX_PROPERTY_MAX_SIZE = 64 * 1024 * 1024;
		vertex_t MEMORY_MERGE_NUM = 8192;
		vertex_t FILE_MERGE_NUM = 32;
		size_t READ_BUFFER_SIZE = 1024 * 1024;
		size_t WRITE_BUFFER_SIZE = 1024 * 1024;
		size_t NUM_OF_COMPACTION_THREAD = 16;
		size_t QUERY_LIST_SIZE = 160;
		size_t MAX_FILE_READER_CACHE_SIZE = 0;
		size_t MAX_WORKER_THREAD = 16;
		double FALSE_POSITIVE = 0.01;
		size_t MAX_LEVEL = 5;
		size_t LEVEL_SIZE_RITIO = 10;

		size_t MAX_BLOCK_SIZE = 1024 * 1024;
	};
#endif // OPTION

}