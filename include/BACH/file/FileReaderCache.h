#pragma once

#include <atomic>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>
#include "FileReader.h"
#include "BACH/utils/Options.h"

namespace BACH
{
	struct FileMetaData;
	class FileReaderCache
	{
	public:
		FileReaderCache(idx_t _max_size, std::string _prefix);
		FileReader* find(FileMetaData* file_data);
		void deletecache(FileMetaData* file_data);
	private:
		std::vector<FileMetaData*> cache;
		std::vector<std::atomic<bool>> cache_deleting;
		std::atomic<idx_t> size = 0;
		idx_t max_size;
		std::string prefix;
		bool no_cache = false;
	};
}