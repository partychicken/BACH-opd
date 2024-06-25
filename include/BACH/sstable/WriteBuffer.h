
#include <memory>
#include <vector>
#include <unistd.h>
#include "Buffer.h"
#include "BACH/file/FileReader.h"
#include "BACH/file/FileWriter.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/options.h"

#pragma once

namespace BACH
{
	//read [b_offset,e_offset)
	template<typename T, typename T_index>
	class WriteBuffer
	{
	public:
		WriteBuffer(std::string file, std::shared_ptr<Options> _options, size_t size = sizeof(T)) :
			file_path(file),
			options(_options),
			buffer_num(_options->WRITE_BUFFER_SIZE / size),
			entry_size(size)
		{
			buffer.resize(buffer_num);
		}
		~WriteBuffer()
		{
			if (has_written)
			{
				writer = NULL;
				disk_buffer = NULL;
				unlink(file_path.c_str());
			}
		}
		T GetNow() const;
		void Next();
		T_index GetNowIndex()const { return read_index; }
		void Write(T x);
		void SetEntrySize(size_t size) { entry_size = size; };
	private:
		std::string file_path;
		std::shared_ptr<Options> options;
		T_index buffer_num;
		size_t entry_size;
		std::vector<T>buffer;
		T_index buffer_cnt = 0;
		T_index write_index = 0;
		T_index read_index = 0;
		T_index total_read_index = 0;
		std::shared_ptr <FileWriter> writer = NULL;
		std::shared_ptr <Buffer<T, T_index>> disk_buffer = NULL;
		bool has_written = false;
	};

	template<typename T, typename T_index>
	inline T WriteBuffer<T, T_index>::GetNow() const
	{
		if (has_written)
		{
			if (read_index >= buffer_cnt * buffer_num)
				return buffer[read_index - buffer_cnt * buffer_num];
			else
				return disk_buffer->GetNow();
		}
		return buffer[read_index];
	}
	template<typename T, typename T_index>
	void WriteBuffer<T, T_index>::Next()
	{
		if (has_written)
		{
			disk_buffer->Next();
		}
		++read_index;
	}
	template<typename T, typename T_index>
	void WriteBuffer<T, T_index>::Write(T x)
	{
		if (write_index == buffer_num)
		{
			if (!has_written)
			{
				writer = std::make_shared<FileWriter>(file_path);
			}
			std::string temp_data;
			for (auto& i : buffer)
			{
				util::PutFixed(temp_data, i);
			}
			writer->append(temp_data.data(), temp_data.size(), true);
			write_index = 0;
			if (!has_written)
			{
				disk_buffer = std::make_shared<Buffer<T,T_index>>(
					std::make_shared<FileReader>(file_path), 0,
					options->WRITE_BUFFER_SIZE, options);
				has_written = true;
			}
			++buffer_cnt;
			disk_buffer->SetSize(buffer_cnt * options->WRITE_BUFFER_SIZE);
		}
		buffer[write_index++] = x;
	}
}