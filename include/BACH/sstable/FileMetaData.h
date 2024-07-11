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
		size_t file_size;
		bool deletion = false;
		bool merging = false;
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
		FileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
			idx_t _file_id, std::string_view label_name) :
			label(_label), level(_level), vertex_id_b(_vertex_id_b),
			file_id(_file_id)
		{
			file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
		}
	};
	bool operator < (FileMetaData* lhs, const std::pair<vertex_t, idx_t>& rhs)
	{
		return lhs->vertex_id_b == rhs.first ?
			lhs->file_id < rhs.second : lhs->vertex_id_b < rhs.first;
	}
}