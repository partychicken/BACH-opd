#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <sul/dynamic_bitset.hpp>
#include "BACH/file/FileReader.h"
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
		sul::dynamic_bitset<>* filter = NULL;
		std::atomic<FileReader*> reader = NULL;
		idx_t reader_pos = -1;
		size_t id = 0;

		FileMetaData() = default;
		FileMetaData(FileMetaData&& x) :
			file_name(x.file_name), label(x.label), level(x.level),
			vertex_id_b(x.vertex_id_b), file_id(x.file_id), ref(x.ref.load()),
			file_size(x.file_size), deletion(x.deletion), filter(x.filter),
			reader(x.reader.load()), id(x.id) {}
		FileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
			idx_t _file_id, std::string_view label_name) :
			label(_label), level(_level), vertex_id_b(_vertex_id_b),
			file_id(_file_id), id((size_t)label << 56 | (size_t)level << 48 | (size_t)vertex_id_b << 16 | (size_t)file_id)
		{
			file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
		}
	};
}