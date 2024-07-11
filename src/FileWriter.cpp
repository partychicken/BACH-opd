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
	// file_path类似于"/a/b/c.sstfile"、"/a/b/c.walfile"
	// 为简化代码，file_path必须为绝对路径
	FileWriter::FileWriter(const std::string& file_path, bool append) {
		int mode = O_CREAT | O_WRONLY;
		if (append) {
			mode |= O_APPEND;
		}
		else {
			mode |= O_TRUNC;
		}
		// 判断文件的目录是否存在， 不存在则创建对应目录
		if (access(file_path.c_str(), F_OK) != F_OK) {
			int idx = (int)file_path.rfind('/');
			std::string dir_path = file_path.substr(0, idx);
			std::filesystem::create_directories(dir_path.c_str());
		}
		this->fd = open(file_path.c_str(), mode, 0644);
		assert(access(file_path.c_str(), F_OK) == F_OK);

	}

	FileWriter::~FileWriter() {
		// 保证buffer中的剩余部分刷盘
		sync();
		close();
	}

	bool FileWriter::append(const char* data, int32_t len, bool flush) {
		int32_t data_offset = 0; // data拷贝的偏移量
		assert(data != nullptr);
		if (len == 0) {
			return true;
		}
		int remain_buf_size = BUFFER_SIZE - buffer_offset;
		if (len < remain_buf_size) {
			// 缓冲区足够，直接拷贝到缓冲区即可
			memcpy(buffer + buffer_offset, data + data_offset, len);
			buffer_offset += len;
			data_offset += len;
		}
		else {
			// 缓冲区不够
			memcpy(buffer + buffer_offset, data, remain_buf_size);
			len -= remain_buf_size;
			buffer_offset += remain_buf_size;
			data_offset += remain_buf_size;
			auto rsp = buf_persist(buffer, BUFFER_SIZE);  // 持久化缓冲区
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

	/*
	 * C库缓冲-----flush---------〉内核缓冲--------fsync-----〉磁盘
	 * */
	void FileWriter::sync() {
		flush(); // 刷到库缓冲区
		fsync(fd);
	}

	void FileWriter::close() {
		flush();
		::close(fd);
		fd = -1;
	}

	bool FileWriter::buf_persist(const char* data, int32_t len) {
		auto ret = write(fd, data, len);
		// todo: 此处应该检查已经写入的ret，没有完全写完则应该继续写，而不是用assert(ret == len);
		//  此外在测试中还发现此处可能写入失败，ret=-1
		cnt += ret;
		assert(ret == len);
		buffer_offset = 0; // flush buf后，需要设置offset为0
		return true;
	}
}