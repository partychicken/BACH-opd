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
	class FileReader final {
	public:
		FileReader(const std::string& file_path, size_t id = 0);
		FileReader(const FileReader& x) = delete;
		FileReader(FileReader&& x) = delete;
		~FileReader();

		bool AddRef();
		void DecRef();
		size_t get_id() const { return id; }

		bool fread(void* buf, size_t count, size_t offset = 0) const;
		bool rread(void* buf, size_t count, size_t offset = 0) const;
		off_t file_size() const { return sb.st_size; }
	private:
		int32_t fd = 0;
		size_t id = 0;
		std::atomic<idx_t> ref = 1;
		struct stat sb {};
	};
}

