#pragma once

#include <vector>
#include "FileMetaData.h"
#include "BACH/memory/VertexEntry.h"

namespace BACH
{
	struct Compaction
	{
		label_t label_id;
		std::vector<FileMetaData> file_list;
		idx_t target_level;
		vertex_t vertex_id_b;
		vertex_t vertex_id_e;
		idx_t chunk_id;
		SizeEntry* Persistence;
	};
}