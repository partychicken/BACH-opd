#pragma once

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include "BloomFilter.h"
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/Options.h"
#include "BACH/memory/VertexEntry.h"
#include "BACH/common/tuple.h"

#pragma once

namespace BACH
{
    template<typename Key_t>
	class BlockParser
	{
	public:
		BlockParser( std::shared_ptr<FileReader> _fileReader,
			std::shared_ptr<Options> _options);
		edge_property_t GetEdge(vertex_t src, vertex_t dst);
        Tuple GetTuple(Key_t key);
        Key_t* GetKeyCol();
        idx_t* GetValCol(int col_id);
        size_t GetKeySize() {
            return key_size;
        }
        size_t GetValueSize() {
            return val_size;
        }
        idx_t GetKeyNum() {
            return key_num;
        }
        idx_t GetColumnNum() {
            return col_num;
        }

	private:
		label_t label;
		std::shared_ptr<FileReader> reader;
		std::shared_ptr<Options> options;
		//std::shared_ptr <BloomFilter> filter = NULL;
        idx_t key_num = 0, col_num = 0;
        size_t key_size = 0, val_size = 0;
		size_t edge_msg_end_pos = 0;//end position
		size_t edge_allocation_end_pos = 0;
		size_t filter_allocation_end_pos = 0;
		size_t filter_end_pos = 0;

		vertex_t now_src = 0;
		off_t file_size = 0;
		size_t src_edge_info_offset = 0;
		edge_len_t src_edge_len = 0;
		std::string edge_allocation_read_buffer;
		std::string edge_msg_read_buffer;
		size_t edge_allocation_buffer_pos = 0, edge_msg_buffer_pos = 0;
		size_t edge_allocation_now_pos = 0, edge_msg_now_pos = 0;
		size_t edge_allocation_buffer_len = 0, edge_msg_buffer_len = 0;
		vertex_t now_edge_src, now_edge_dst;
		edge_property_t now_edge_prop;
		bool valid = true;
	};
}