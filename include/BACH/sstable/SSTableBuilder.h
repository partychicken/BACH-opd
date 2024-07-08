#pragma once

#include <memory>
#include <vector>
#include "BloomFilter.h"
#include "BACH/file/FileWriter.h"
#include "BACH/memory/EdgeEntry.h"
#include "BACH/sstable/DstEntry.h"
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"

namespace BACH
{
	class SSTableBuilder
	{
	public:
		explicit SSTableBuilder(std::shared_ptr<FileWriter> _fileWriter);
		~SSTableBuilder() = default;
		void AddFilter(idx_t keys_num, double false_positive);
		//void SetRange(vertex_t b, vertex_t e);
		void FinishSrc();
		void AddDst(vertex_t src, vertex_t dst);
		void FinishDst();
		void AddEdge(EdgeEntry& e);
		void FinishSST();
	private:
		std::shared_ptr <FileWriter> writer;
		std::vector <std::shared_ptr <BloomFilter>> filter;
		std::vector <size_t> filter_offset;
		std::vector <vertex_t> src_index;
		std::vector <time_t> min_t;
		std::vector <time_t> max_t;
		std::string temp_data;
		size_t offset_info[7];
		vertex_t src_b = 0, src_e = 0;
		vertex_t src_cnt = 0;
		edge_t ver_cnt = 0;
		time_t now_min_t = TOMBSTONE, now_max_t = 0;
		time_t all_min_t = TOMBSTONE, all_max_t = 0;
		vertex_t dst_cnt = 0;
		vertex_t now_dst = 0;
		size_t offset = 0;
		template<typename T>
		inline void append_data(std::vector<T> x)
		{
			std::string temp_data;
			for (idx_t i = 0; i < x.size(); ++i)
				util::PutFixed(temp_data, x[i]);
			writer->append(temp_data.data(), temp_data.size());
		}
		inline void append_src_data()
		{
			std::string temp_data;
			for (idx_t i = 0; i < src_index.size(); ++i)
			{
				util::PutFixed(temp_data, src_index[i]);
				util::PutFixed(temp_data, min_t[i]);
				util::PutFixed(temp_data, max_t[i]);
			}
			writer->append(temp_data.data(), temp_data.size());
		}
	};
}