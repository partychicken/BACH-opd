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
		void GetEdges(vertex_t src, std::shared_ptr<std::vector<
			std::tuple<vertex_t, vertex_t, edge_property_t>>> answer,
			bool (*func)(edge_property_t));
		void GetEdgeRangeBySrcId(vertex_t src);
		vertex_t GetSrcBegin()const { return src_b; }
		vertex_t GetSrcEnd()const { return src_e; }
		edge_t GetEdgeNum()const { return edge_cnt; }
		void BatchReadEdge();
		vertex_t GetNowSrc()const;
		vertex_t GetSrcIndex(vertex_t src)const { return src_index[src - src_b]; };
		std::shared_ptr<DstEntry> GetNowDst()const;
		edge_t GetNowVerCount()const;
		std::shared_ptr<EdgeEntry> GetNowEdge(bool force = false)const;
		void Reset(bool reset_dst = true);
		void NextSrc();
		bool NextDst();
		bool NextEdge(bool force = false);
		void BatchQuery(time_t now_time, edge_property_t edge_property,
			std::shared_ptr<sul::dynamic_bitset<>> answer_bitset,
			std::shared_ptr<sul::dynamic_bitset<>> filter_bitset);

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
		vertex_t now_src_p = 0;
		off_t file_size = 0;
		size_t src_edge_info_offset = 0;
		edge_len_t src_edge_len = 0;
		std::shared_ptr<Buffer<EdgeEntry, edge_num_t>> edge_buffer = NULL;
	};
}