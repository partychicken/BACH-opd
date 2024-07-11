#pragma once

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include "BloomFilter.h"
#include "Buffer.h"
#include "BACH/db/DB.h"
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/options.h"
#include "BACH/memory/VertexEntry.h"

#pragma once

namespace BACH
{
	class DB;
	class SSTableParser
	{
	public:
		explicit SSTableParser(DB* _db, label_t _label,
			std::shared_ptr<FileReader> _fileReader,
			std::shared_ptr<Options> _options, bool if_read_filter = true);
		~SSTableParser() = default;
		edge_property_t GetEdge(vertex_t src, vertex_t dst);
		void ReadEdgeAllocationBuffer();
		void ReadEdgeMsgBuffer();
		bool GetFirstEdge();
		void GetNextEdge(vertex_t src, std::shared_ptr<std::vector<
			std::tuple<vertex_t, vertex_t, edge_property_t>>> answer,
			bool (*func)(edge_property_t));
		void GetEdgeRangeBySrcId(vertex_t src);
		vertex_t GetSrcBegin()const { return src_b; }
		vertex_t GetSrcEnd()const { return src_e; }
		edge_t GetEdgeNum()const { return edge_cnt; }
		vertex_t GetNowEdgeSrc()const {return now_edge_src; }	
		vertex_t GetNowEdgeDst()const {return now_edge_dst; }	
		edge_property_t GetNowEdgeProp()const {return now_edge_prop; }	

	private:
		DB* db;
		label_t label;
		std::shared_ptr<FileReader> reader;
		std::shared_ptr<Options> options;
		bool read_filter;
		//std::shared_ptr <BloomFilter> filter = NULL;
		edge_t edge_cnt = 0;
		size_t edge_msg_end_pos = 0;//end position
		size_t edge_allocation_end_pos = 0;
		size_t filter_end_pos = 0;
		size_t filter_allocation_end_pos = 0;
		vertex_t src_b = 0, src_e = 0;
		vertex_t now_src = 0;
		off_t file_size = 0;
		size_t src_edge_info_offset = 0;
		edge_len_t src_edge_len = 0;
		char* edge_allocation_read_buffer = NULL;
		char* edge_msg_read_buffer = NULL;
		size_t edge_allocation_buffer_pos = 0, edge_msg_buffer_pos = 0;
		size_t edge_allocation_now_pos = 0, edge_msg_now_pos = 0;
		size_t edge_allocation_buffer_len = 0, edge_msg_buffer_len = 0;
		vertex_t now_edge_src,now_edge_dst;
		edge_property_t now_edge_prop;
	};
}