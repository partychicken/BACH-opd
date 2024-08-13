#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <BACH/file/FileReader.h>
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
		std::atomic<idx_t> ref = 0;
		size_t file_size;
		bool deletion = false;
		bool merging = false;
		bool death = false;
		identify_t identify;
		std::atomic<FileReader*> reader = NULL;
		idx_t reader_pos = -1;

		FileMetaData() = default;
		FileMetaData(const FileMetaData& x) :
			file_name(x.file_name), label(x.label), level(x.level),
			vertex_id_b(x.vertex_id_b), file_id(x.file_id), ref(x.ref.load()),
			file_size(x.file_size), deletion(x.deletion), identify(x.identify) {}
		FileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
			idx_t _file_id, std::string_view label_name, identify_t _identify) :
			label(_label), level(_level), vertex_id_b(_vertex_id_b),
			file_id(_file_id), identify(_identify)
		{
			file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
		}
	};
}