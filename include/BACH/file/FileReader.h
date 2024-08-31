#pragma once

#include <atomic>
#include <cassert>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "BACH/utils/utils.h"

namespace BACH {
	struct FileMetaData;
	class FileReader final {
	public:
		FileReader(const std::string& file_path/*, FileMetaData* _file_data = NULL*/);
		FileReader(const FileReader& x) = delete;
		FileReader(FileReader&& x) = delete;

		~FileReader();
		//void add_ref();
		//bool dec_ref(identify_t id, bool deadby = false);
		// 类似unistd中的pread
		// 从offset处开始，读取count长度的内容到buf中
		bool fread(void* buf, size_t count, size_t offset = 0) const;
		// 从filesize-offset处开始，读取count长度的内容到buf中
		bool rread(void* buf, size_t count, size_t offset = 0) const;
		off_t file_size() const { return sb.st_size; }
	private:
		int32_t fd = 0;
		//std::atomic<bool> deleted = false;
		//FileMetaData* file_data = NULL;
		//identify_t id;
		//std::atomic<ref_t> ref;
		struct stat sb {};
		friend class FileReaderCache;
		//friend class SSTableParser;
	};


}

