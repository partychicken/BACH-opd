#pragma once

#include <string>
#include <string_view>
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"

namespace BACH
{
	struct FileMetaData
	{
		std::string file_name;
		label_t label;
		idx_t level;
		vertex_t vertex_id_b;
		vertex_t vertex_id_e;
		idx_t file_id;
		idx_t refs = 0;
		FileMetaData() = default;
		FileMetaData(std::string_view _file_name, vertex_t size):
			file_name(_file_name)
		{
			int idx1 = file_name.find('_');
			label = file_name.substr(0, idx1);
			sscanf(file_name.substr(idx1 + 1).data(),
				"%d_%d_%d", &level, &vertex_id_b, &file_id);
			vertex_id_e = vertex_id_b + size - 1;
			create_time = time;
		}
		FileMetaData(label_t label, idx_t level, vertex_t vertex_id_b,
			vertex_t vertex_id_e, idx_t file_id, std::string label_name) :
			label(label), level(level), vertex_id_b(vertex_id_b),
			vertex_id_e(vertex_id_e), file_id(file_id)
		{
			file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
		}
	};
}