#include "BACH/file/FileReader.h"
#include "BACH/sstable/FileMetaData.h"

namespace BACH {
	FileReader::FileReader(const std::string& file_path, size_t id)	: id(id) {
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

	bool FileReader::AddRef()
	{
		idx_t k;
		do
		{
			k = ref.load();
			if(k == 0)
				return false;
		} while (!ref.compare_exchange_weak(k, k + 1));
		return true;
	}
	void FileReader::DecRef()
	{
		auto k = ref.fetch_add(-1);
		if (k == 1)
			delete this;
	}

	bool FileReader::fread(void* buf, size_t count, size_t offset) const {
		if (buf == nullptr) {
			return false;
		}
		if (fd == -1) {
			return false;
		}

		size_t cnt = pread(fd, buf, count, offset);
		if (cnt != count) {
			cnt = pread(fd, buf, count, offset);
			return false;
		}
		return true;
	}

	bool FileReader::rread(void* buf, size_t count, size_t offset) const {
		return fread(buf, count, sb.st_size - offset);
	}
}