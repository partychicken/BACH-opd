#include <string>
#include <unistd.h>
#include <cassert>
#include <fcntl.h>
#include "BACH/file/FileReader.h"
#include <iomanip>
#include <iostream>
namespace BACH {
	FileReader::FileReader(const std::string& file_path) {
		fd = open(file_path.c_str(), O_RDONLY);
		if (fd == -1)
		{
			std::cout << "cannot open file " << file_path << std::endl;
		}
		if (fd != -1)
		{
			fstat(fd, &sb);
		}
	}

	FileReader::~FileReader() {
		if (fd != -1) {
			close(fd);
			fd = -1;
		}
	}

	bool FileReader::fread(void* buf, int32_t count, int32_t offset) const {
		if (buf == nullptr) {
			return false;
		}
		if (fd == -1) {
			return false;
		}
		auto cnt = pread(fd, buf, count, offset);
		if (cnt != count) {
			return false;
		}
		return true;
	}

	bool FileReader::rread(void* buf, int32_t count, int32_t offset) const {
		return fread(buf, count, sb.st_size - offset);
	}
}