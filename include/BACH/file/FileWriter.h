#pragma once

#include <string>
#include <cstddef>
#include <cstdint>
#include "BACH/profiler/OperatorProfilerContext.h"
#include "BACH/profiler/profiler.h"
namespace BACH
{
	class FileWriter final {
	private:
		static constexpr int32_t BUFFER_SIZE = 1024 * 128;
		int32_t buffer_offset = 0;
		char buffer[BUFFER_SIZE]{};

		int32_t fd;
		size_t cnt = 0;

	public:
		FileWriter(const std::string& file_path, bool append = false);

		~FileWriter();

		bool append(const char* data, int32_t len, bool flush = false);
		bool flush();
		size_t get_offset() const { return buffer_offset; }
		void sync();
		size_t file_size() const { return cnt; };
		void close();
	private:
		bool buf_persist(const char* data, int32_t len);

		bool is_buffer_full() const {
			return buffer_offset == BUFFER_SIZE;
		}
	};
}