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
		idx_t file_id;
		idx_t ref = 0;
		bool deletion = false;
		FileMetaData() = default;
		//FileMetaData(std::string_view _file_name, vertex_t size,
		//	std::shared_ptr<LabelManager> Labels) :
		//	file_name(_file_name)
		//{
		//	int idx1 = file_name.find('_');
		//	label = Labels->GetEdgeLabelId(file_name.substr(0, idx1));
		//	sscanf(file_name.substr(idx1 + 1).data(),
		//		"%d_%d_%d", &level, &vertex_id_b, &file_id);
		//}
		FileMetaData(label_t label, idx_t level, vertex_t vertex_id_b,
			idx_t file_id, std::string_view label_name) :
			label(label), level(level), vertex_id_b(vertex_id_b),
			file_id(file_id)
		{
			file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
		}
		bool operator < (const FileMetaData& rhs) const
		{
			return vertex_id_b == rhs.vertex_id_b ?
				file_id < rhs.file_id : vertex_id_b < rhs.vertex_id_b;
		}
		bool operator < (const std::pair<vertex_t, idx_t>& rhs) const
		{
			return vertex_id_b == rhs.first ?
				file_id < rhs.second : vertex_id_b < rhs.first;
		}
	};
}