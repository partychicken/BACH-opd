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
		void SetSrcRange(vertex_t src_b, vertex_t src_e);
		void ArrangeCurrentSrcInfo();
		void ArrangeSSTableInfo();
		void AddEdge(vertex_t src, vertex_t dst, edge_property_t edge_property);
	private:
		std::shared_ptr <FileWriter> writer;
		std::vector <std::shared_ptr <BloomFilter>> filter;
		std::vector <size_t> edge_allocation_list;
		std::vector <vertex_t> edge_dst_id_list;
		std::vector <edge_property_t> edge_property_list;
		edge_t edge_cnt = 0;
        size_t edge_msg_end_pos = 0;//end position
		size_t edge_allocation_end_pos = 0;
		size_t filter_end_pos = 0;
        size_t filter_allocation_end_pos = 0;
		edge_num_t src_edge_num = 0;
		vertex_t src_b = 0, src_e = 0;
	};
}