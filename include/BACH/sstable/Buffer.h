#include <memory>
#include <vector>
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/options.h"

#pragma once

namespace BACH
{
	template<typename T, typename T_index>
	class Buffer
	{
	public:
		//read [b_offset,e_offset)
		Buffer(std::shared_ptr <FileReader> fr, size_t b_offset, size_t e_offset, std::shared_ptr<Options> _options, size_t size = sizeof(T)) :
			reader(fr),
			options(_options),
			buffer_num(_options->READ_BUFFER_SIZE / size),
			offset(b_offset),
			begin_offset(b_offset),
			end_offset(e_offset),
			entry_size(size)
		{
			Read();
			first_read = true;
		}
		T GetNow() const;
		T_index GetNowIndex()const { return total_index; }
		void Next();
		void Read();
		void Reset();
		void SetSize(size_t size) { end_offset = size; };
		void SetEntrySize(size_t size) { entry_size = size; };
	private:
		std::shared_ptr <FileReader> reader;
		std::shared_ptr<Options> options;
		T_index buffer_num;
		size_t offset;
		size_t begin_offset, end_offset;
		size_t entry_size;
		std::vector<T>buffer;
		T_index now_index = 0;
		T_index total_index = 0;
		bool first_read;//for reset
	};

	template<typename T, typename T_index>
	inline T Buffer<T, T_index>::GetNow() const
	{
		if (now_index < buffer.size())
			return buffer[now_index];
		//error
		return T();
	}
	template<typename T, typename T_index>
	void Buffer<T, T_index>::Next()
	{
		if (now_index + 1 >= buffer.size())
			Read();
		++now_index;
		++total_index;
		offset += entry_size;
	}
	template<typename T, typename T_index>
	void Buffer<T, T_index>::Read()
	{
		size_t cnt = std::min(options->READ_BUFFER_SIZE,
			(end_offset - offset)) / entry_size;
		if (cnt <= 0)
			return;
		char buf[cnt * entry_size];
		reader->fread(buf, cnt * entry_size, offset);
		if (buffer.size() < cnt)
			buffer.resize(cnt);
		for (T_index i = 0; i < cnt; ++i)
		{
			util::DecodeFixed(buf + entry_size * i, buffer[i]);
		}
		now_index = 0;
		first_read = false;
	}
	template<typename T, typename T_index>
	void Buffer<T, T_index>::Reset()
	{
		offset = begin_offset;
		now_index = total_index = 0;
		if (!first_read)
		{
			Read();
			first_read = true;
		}
	}
}