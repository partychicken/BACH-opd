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

		/*if(count==36)
		{
			char buffer[count + 20];
			auto cnt1 = pread(fd, buffer, count + 20, offset - 10);
			std::cout << cnt1 << std::endl;
			for (int i=0;i<count+20;++i)
				std::cout << std::setfill('0') << std::setw(2)
				<< std::hex << (unsigned int)(unsigned char)buffer[i] << std::dec;
			std::cout << std::endl;
		}*/
		auto cnt = pread(fd, buf, count, offset);
		if (cnt != count) {
			return false;
		}
		return true;
	}

	bool FileReader::rread(void* buf, int32_t count, int32_t offset) const {
		//std::cout<<"rs" << sb.st_size << " " << offset << std::endl;
		return fread(buf, count, sb.st_size - offset);
	}
}