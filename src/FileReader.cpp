#include "BACH/file/FileReader.h"
#include "BACH/sstable/FileMetaData.h"

namespace BACH {
	FileReader::FileReader(const std::string& file_path) {
		if (!file_path.empty())
		{
			fd = open(file_path.c_str(), O_RDONLY);
			if (fd == -1)
			{
				std::cout << "cannot open file " << file_path << std::endl;
			}
			else
			{
				fstat(fd, &sb);
			}
		}
		else
			fd = -1;
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
			cnt = pread(fd, buf, count, offset);
			return false;
		}
		return true;
	}

	bool FileReader::rread(void* buf, int32_t count, int32_t offset) const {
		return fread(buf, count, sb.st_size - offset);
	}
}