#include <filesystem>
#include <cassert>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <unistd.h>
#include "BACH/file/FileWriter.h"

namespace BACH
{
	FileWriter::FileWriter(const std::string& file_path, bool append) {
		int mode = O_CREAT | O_WRONLY;
		if (append) {
			mode |= O_APPEND;
		}
		else {
			mode |= O_TRUNC;
		}
		if (access(file_path.c_str(), F_OK) != F_OK) {
			int idx = (int)file_path.rfind('/');
			std::string dir_path = file_path.substr(0, idx);
			std::filesystem::create_directories(dir_path.c_str());
		}
		this->fd = open(file_path.c_str(), mode, 0644);
		assert(access(file_path.c_str(), F_OK) == F_OK);

	}

	FileWriter::~FileWriter() {
		sync();
		close();
	}

	bool FileWriter::append(const char* data, int32_t len, bool flush) {
		int32_t data_offset = 0;
		assert(data != nullptr);
		if (len == 0) {
			return true;
		}
		int remain_buf_size = BUFFER_SIZE - buffer_offset;
		if (len < remain_buf_size) {
			memcpy(buffer + buffer_offset, data + data_offset, len);
			buffer_offset += len;
			data_offset += len;
		}
		else {
			memcpy(buffer + buffer_offset, data, remain_buf_size);
			len -= remain_buf_size;
			buffer_offset += remain_buf_size;
			data_offset += remain_buf_size;
			auto rsp = buf_persist(buffer, BUFFER_SIZE);
			assert(rsp == true);

			while (len > 0) {
				if (is_buffer_full()) {
					rsp = buf_persist(buffer, BUFFER_SIZE);
					assert(rsp == true);
				}
				int need_cpy = std::min(len, BUFFER_SIZE - buffer_offset);
				memcpy(buffer + buffer_offset, data + data_offset, need_cpy);
				data_offset += need_cpy;
				len -= need_cpy;
				buffer_offset += need_cpy;
				if (len == 0) {
					break;
				}
			}
		}
		if (flush) {
			buf_persist(buffer, buffer_offset);
		}
		return true;
	}

	bool FileWriter::flush() {
		if (buffer_offset > 0) {
			auto rsp = buf_persist(buffer, buffer_offset);
			assert(rsp == true);
		}
		return true;
	}

	void FileWriter::sync() {
		flush();
		fsync(fd);
	}

	void FileWriter::close() {
		flush();
		::close(fd);
		fd = -1;
	}

	bool FileWriter::buf_persist(const char* data, int32_t len) {
		auto ret = write(fd, data, len);
		cnt += ret;
		assert(ret == len);
		buffer_offset = 0;
		return true;
	}
}