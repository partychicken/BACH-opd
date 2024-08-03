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
		size_t MEM_TABLE_MAX_SIZE = 4 * 1024;
		size_t VERTEX_PROPERTY_MAX_SIZE = 64 * 1024 * 1024;
		vertex_t MERGE_NUM = 32;
		size_t READ_BUFFER_SIZE = 1024 * 1024;
		size_t WRITE_BUFFER_SIZE = 1024 * 1024;
		size_t NUM_OF_COMPACTION_THREAD = 1;
		size_t QUERY_LIST_SIZE = 160;
		double FALSE_POSITIVE = 0.01;
	};
#endif // OPTION

}