#pragma once

#include <atomic>
#include <memory>
#include <string_view>
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
		//void erase(idx_t pos);
		//std::shared_ptr<FileReader> add(FileMetaData* file_data);
	private:
		std::vector<FileMetaData*> cache;
		std::vector<std::atomic<bool>> cache_deleting;
		std::atomic<idx_t> size =  0;
		idx_t max_size;
		std::string prefix;
		//inline FileReader* wait(FileMetaData*& file_data);
		inline void remove(idx_t pos);
	};
}