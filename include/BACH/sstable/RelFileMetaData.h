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
#include "BACH/common/dictionary.h"

namespace BACH
{
	template <typename Key_t>
	struct RelFileMetaData
	{
		std::string file_name;
		idx_t level;
		idx_t file_id;
		std::atomic<idx_t> ref = 0;
		size_t file_size;
		bool deletion = false;
		bool merging = false;
		sul::dynamic_bitset<>* filter = NULL;
		std::atomic<FileReader*> reader = NULL;
		idx_t reader_pos = -1;
		size_t id = 0;
		std::shared_ptr<Dict<Key_t> > dictionary = nullptr;
		Key_t key_min, key_max;

		RelFileMetaData() = default;
		RelFileMetaData(RelFileMetaData&& x) :
			file_name(x.file_name), level(x.level), file_id(x.file_id), ref(x.ref.load()),
			file_size(x.file_size), deletion(x.deletion), filter(x.filter),
			reader(x.reader.load()), id(x.id)
			//,dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max)
			{}

		RelFileMetaData(idx_t _level, Key_t _key_min, idx_t _file_id) :
			level(_level), key_min(_key_min), file_id(_file_id), id(static_cast<size_t>(level) << 48 | static_cast<size_t>(file_id)) {
			file_name = util::BuildSSTPath(0, level, 0, file_id);
		}
	};
}