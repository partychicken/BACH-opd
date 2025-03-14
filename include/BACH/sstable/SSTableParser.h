#pragma once

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <sul/dynamic_bitset.hpp>
#include "BloomFilter.h"
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/Options.h"
#include "BACH/memory/VertexEntry.h"

#pragma once

namespace BACH
{
	class SSTableParser
	{
	public:
		SSTableParser(label_t _label,
			FileReader* _fileReader,
			std::shared_ptr<Options> _options);
		SSTableParser(SSTableParser&& x);
		~SSTableParser();
		edge_property_t GetEdge(vertex_t src, vertex_t dst);
		void GetEdges(vertex_t src, std::shared_ptr<std::vector<
			std::pair<vertex_t, edge_property_t>>> answer,
			const std::function<bool(edge_property_t&)>&func);
		void ReadEdgeAllocationBuffer();
		void ReadEdgeMsgBuffer();
		bool GetFirstEdge();
		bool GetNextEdge();
		void GetEdgeRangeBySrcId(vertex_t src);
		vertex_t GetSrcBegin()const { return src_b; }
		vertex_t GetSrcEnd()const { return src_e; }
		edge_t GetEdgeNum()const { return edge_cnt; }
		vertex_t GetNowEdgeSrc()const { return now_edge_src; }
		vertex_t GetNowEdgeDst()const { return now_edge_dst; }
		edge_property_t GetNowEdgeProp()const { return now_edge_prop; }

	private:
		label_t label;
		FileReader* reader;
		std::shared_ptr<Options> options;
		//std::shared_ptr <BloomFilter> filter = NULL;
		edge_t edge_cnt = 0;
		size_t edge_msg_end_pos = 0;//end position
		size_t edge_allocation_end_pos = 0;
		size_t filter_allocation_end_pos = 0;
		size_t filter_end_pos = 0;
		vertex_t src_b = 0, src_e = 0;
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