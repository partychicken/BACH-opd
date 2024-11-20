#pragma once

#include <vector>
#include "FileMetaData.h"
#include "BACH/memory/VertexEntry.h"

namespace BACH
{
	struct Compaction
	{
		label_t label_id;
		std::vector<FileMetaData*> file_list;
		idx_t target_level;
		vertex_t vertex_id_b;
		idx_t file_id;
		std::shared_ptr < SizeEntry > Persistence;
	};
}