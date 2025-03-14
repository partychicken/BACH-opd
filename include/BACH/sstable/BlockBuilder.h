#pragma once

#include <memory>
#include <vector>
#include "BloomFilter.h"
#include "BACH/file/FileWriter.h"
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"

namespace BACH
{
    template <typename Key_t>
	class BlockBuilder 
	{
	public:
		explicit BlockBuilder(std::shared_ptr<FileWriter> _fileWriter,std::shared_ptr<Options> _options);
		~BlockBuilder() = default;
		//void AddFilter(idx_t keys_num, double false_positive);
		void SetKeyRange(Key_t key_min, Key_t key_max);
		void ArrangeBlockInfo();
        void SetKey();
        void SetValueIdx();
	private:
		std::shared_ptr <FileWriter> writer;
		std::shared_ptr<Options> options;
		//std::vector <std::shared_ptr <BloomFilter>> filter;
		std::vector <size_t> edge_allocation_list;
		std::vector <vertex_t> edge_dst_id_list;
		std::vector <edge_property_t> edge_property_list;
		edge_t edge_cnt = 0;
        size_t edge_msg_end_pos = 0;//end position
		size_t edge_allocation_end_pos = 0;
		size_t filter_end_pos = 0;
        size_t filter_allocation_end_pos = 0;
		edge_num_t src_edge_num = 0;
        Key_t key_min, key_max;
	};
}