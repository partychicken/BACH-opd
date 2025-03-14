#include "BACH/sstable/SSTableParser.h"
#include "BACH/sstable/FileMetaData.h"
namespace BACH
{
	SSTableParser::SSTableParser(label_t _label,
		FileReader* _fileReader,
		std::shared_ptr<Options> _options) :
		label(_label),
		reader(_fileReader),
		options(_options)
	{
		file_size = reader->file_size();

		//SSTable用CSR块存储信息，src_vertex->dst_vertex
		size_t offset = sizeof(vertex_t) * 2 + sizeof(edge_num_t);
		char infobuf[offset];
		if (!reader->rread(infobuf, offset, offset))
		{
			std::cout << "read fail begin" << std::endl;
			++*(int *)NULL;
		}
		util::DecodeFixed(infobuf, src_b);
		util::DecodeFixed(infobuf + sizeof(vertex_t), src_e);
		util::DecodeFixed(infobuf + sizeof(vertex_t) * 2, edge_cnt);
		// SStable最后部分是三个参数，其中src_b和src_e代表当前文件src_vertex的起始和终止的vertex_id的范围;edge_cnt代表边的总数量;

		edge_msg_end_pos = (sizeof(vertex_t) + sizeof(edge_property_t)) * edge_cnt;
		// 第一部分是边的信息，每条边包括dst_vertex_id和这条边代表的边属性，边属性暂定为定长的double类型

		edge_allocation_end_pos = edge_msg_end_pos + sizeof(edge_num_t) * (src_e - src_b + 1);
		// 第二部分是记录src对应的dst在边数组中的位置范围，例如src为10对应的dst范围为( edge_list[src_info_list[src-1]],edge_list[src_info_list[src]] ]

		//filter_allocation_end_pos = file_size - sizeof(vertex_t) * 2 - sizeof(edge_num_t);
		// 第四部分是布隆过滤器的分配数组，长度为固定长度，即代表文件中能表示的src_vertex_id的范围，然后每个字段为（filter_len，hash_func_num）,就可以计算出布隆过滤器的数组长度 

		//filter_end_pos = filter_allocation_end_pos - (sizeof(size_t) + sizeof(idx_t)) * (src_e - src_b + 1);
		// 第三部分是布隆过滤器的信息，每一个src分配一个布隆过滤器，布隆过滤器不是定长的，因此需要第四部分的过滤器分配数组来标识src对应的布隆过滤器,并且由filter_len来标识过滤器长度
	}
	SSTableParser::SSTableParser(SSTableParser &&x):
		label(x.label), reader(x.reader), options(x.options),
		edge_cnt(x.edge_cnt), edge_msg_end_pos(x.edge_msg_end_pos),
		edge_allocation_end_pos(x.edge_allocation_end_pos),
		src_b(x.src_b), src_e(x.src_e), file_size(x.file_size)
	{
		x.valid = false;
	}
	
	SSTableParser::~SSTableParser()
	{
		if(valid)
			reader->DecRef();
	}
	// 在此文件中查找特定src->dst的边，如果不存在则返回null，存在返回一个指向(vertex_id,edge_property)的指针
	edge_property_t SSTableParser::GetEdge(vertex_t src, vertex_t dst)
	{
		// 通过布隆过滤器分配数组，拿到布隆过滤器date的存放位置（存放位置是一个区间，因此需要取布隆过滤器分配数组的src-1以及src的信息）以及布隆过滤器的func_num
		//size_t offset, len;
		//idx_t func_num;
		if (src > src_e || src < src_b)
			return NOTFOUND;
		//if (src == this->src_b)
		//{
		//	char filter_allocation_info[singel_filter_allocation_size];
		//	if (!reader->fread(filter_allocation_info, singel_filter_allocation_size, filter_end_pos))
		//	{
		//		std::cout << "read fail src_b" << std::endl;
		//	}
		//	offset = 0;
		//	len = util::GetDecodeFixed<size_t>(filter_allocation_info);
		//	func_num = util::GetDecodeFixed<idx_t>(filter_allocation_info + sizeof(size_t));
		//}
		//else {
		//	char filter_allocation_info[singel_filter_allocation_size * 2];
		//	if (!reader->fread(filter_allocation_info, singel_filter_allocation_size * 2, filter_end_pos + (src - src_b - 1) * singel_filter_allocation_size))
		//	{
		//		std::cout << "read fail src" << std::endl;
		//	}
		//	offset = util::GetDecodeFixed<size_t>(filter_allocation_info);
		//	len = util::GetDecodeFixed<size_t>(filter_allocation_info + singel_filter_allocation_size) - offset;
		//	func_num = util::GetDecodeFixed<idx_t>(filter_allocation_info + singel_filter_allocation_size + sizeof(size_t));
		//}

		// query 如果布隆过滤器长度为0直接证明没有src的边
		//if (!len)
		//{
		//	return NOTFOUND;
		//}

		// 得到布隆过滤器的位置和长度之后在存放布隆过滤器信息的数组中拿到相应的布隆过滤器data
		//auto filter = std::make_shared<BloomFilter>();
		//std::string filter_data;
		//filter_data.resize(len);
		//if (!reader->fread(filter_data.data(), len, offset + edge_allocation_end_pos)) {
		//	std::cout << "read fail bloom" << std::endl;
		//}
		//filter->create_from_data(func_num, filter_data);
		//if (!filter->exists(dst))
		//	return NOTFOUND;

		GetEdgeRangeBySrcId(src);
		// 批量读边，检查是否存在src->dst这条边
		size_t singel_read_max_len = this->options->READ_BUFFER_SIZE / singel_edge_total_info_size * singel_edge_total_info_size;
		edge_len_t read_num = (this->src_edge_len - 1) / singel_read_max_len + 1;
		for (edge_len_t i = 1; i <= read_num; i++)
		{
			size_t len = (i != read_num) ? singel_read_max_len : this->src_edge_len % singel_read_max_len;
			char buffer[len];
			if (!reader->fread(buffer, len, this->src_edge_info_offset)) {
				std::cout << "read fail dst" << std::endl;
				++*(int *)NULL;
			}
			size_t offset = 0;
			this->src_edge_info_offset += len;
			for (size_t j = 0; j < len / singel_edge_total_info_size; j++)
			{
				auto vertex_id = util::GetDecodeFixed<vertex_t>(buffer + offset);
				if (vertex_id == dst) {
					return util::GetDecodeFixed<edge_property_t>(buffer + offset + sizeof(vertex_t));
				}
				offset += singel_edge_total_info_size;
			}
		}
		return NOTFOUND;
	}
	// 在一个文件中批量查找以src为起点的所有边，边属性的条件过滤函数以参数形式放入，过滤之后的边信息放在answer指向的vector中
	void SSTableParser::GetEdges(vertex_t src, std::shared_ptr<std::vector<
		std::pair<vertex_t, edge_property_t>>> answer,
		const std::function<bool(edge_property_t&)>& func)
	{
		GetEdgeRangeBySrcId(src);
		if (!this->src_edge_len)
		{
			return;
		}

		// 批量读边，将边属性过滤后的边放入answer中
		size_t singel_read_max_len = this->options->READ_BUFFER_SIZE / singel_edge_total_info_size * singel_edge_total_info_size;
		edge_len_t read_num = (this->src_edge_len - 1) / singel_read_max_len + 1;
		//answer->reserve(src_edge_len / singel_edge_total_info_size);
		vertex_t answer_size = answer->size();
		vertex_t answer_cnt = 0;
		for (edge_len_t i = 1; i <= read_num; i++)
		{
			size_t len = (i != read_num) ? singel_read_max_len : this->src_edge_len % singel_read_max_len;
			char buffer[len];
			if (!reader->fread(buffer, len, this->src_edge_info_offset)) {
				std::cout << "read fail dsts" << std::endl;
				++*(int *)NULL;
			}
			size_t offset = 0;
			this->src_edge_info_offset += len;
			for (size_t j = 0; j < len / singel_edge_total_info_size; j++)
			{
				auto vertex_id = util::GetDecodeFixed<vertex_t>(buffer + offset);
				//if (!filter[vertex_id])
				{
					auto edge_property = util::GetDecodeFixed<edge_property_t>(buffer + offset + sizeof(vertex_t));
					if (edge_property != TOMBSTONE && func(edge_property))
					{
						if (answer_cnt >= answer_size)
							answer->emplace_back(vertex_id, edge_property);
						else
							(*answer)[answer_cnt++] = std::make_pair(vertex_id, edge_property);
						//answer->emplace_back(vertex_id, edge_property);
					}
					//filter[vertex_id] = true;
				}
				offset += singel_edge_total_info_size;
			}
		}
		if (answer_cnt < answer_size)
		{
			answer->resize(answer_cnt);
		}
	}
	void SSTableParser::GetEdgeRangeBySrcId(vertex_t src)
	{
		if (src == this->src_b)
		{
			size_t offset = this->edge_msg_end_pos;
			size_t len = sizeof(edge_num_t);
			char buffer[len];
			if (!reader->fread(buffer, len, offset))
			{
				std::cout << "read fail range 0" << std::endl;
				++*(int *)NULL;
			}
			this->src_edge_info_offset = 0;
			this->src_edge_len = util::GetDecodeFixed<edge_num_t>(buffer) * singel_edge_total_info_size;
		}
		else
		{
			size_t offset = this->edge_msg_end_pos + (src - this->src_b - 1) * sizeof(edge_num_t);
			size_t len = sizeof(edge_num_t) * 2;
			char buffer[len];
			if (!reader->fread(buffer, len, offset))
			{
				std::cout << "read fail range" << std::endl;
				++*(int *)NULL;
			}
			this->src_edge_info_offset = util::GetDecodeFixed<edge_num_t>(buffer) * singel_edge_total_info_size;
			this->src_edge_len = (util::GetDecodeFixed<edge_num_t>(buffer + sizeof(edge_num_t)) - util::GetDecodeFixed<edge_num_t>(buffer)) * singel_edge_total_info_size;
		}
	}
	void SSTableParser::ReadEdgeAllocationBuffer()
	{
		this->edge_allocation_buffer_pos = 0;
		this->edge_allocation_buffer_len = std::min(this->options->READ_BUFFER_SIZE / sizeof(edge_num_t), this->src_e - this->src_b + 1 - this->edge_allocation_now_pos);
		this->edge_allocation_read_buffer.clear();
		this->edge_allocation_read_buffer.resize(this->edge_allocation_buffer_len * sizeof(edge_num_t));
		if (!reader->fread(edge_allocation_read_buffer.data(), this->edge_allocation_buffer_len * sizeof(edge_num_t), this->edge_msg_end_pos + this->edge_allocation_now_pos * sizeof(edge_num_t)))
		{
			std::cout << "read fail alloca" << std::endl;
			++*(int *)NULL;
		}
	}
	void SSTableParser::ReadEdgeMsgBuffer()
	{
		this->edge_msg_buffer_pos = 0;
		this->edge_msg_buffer_len = std::min(this->options->READ_BUFFER_SIZE / singel_edge_total_info_size, this->edge_cnt - this->edge_msg_now_pos);
		this->edge_msg_read_buffer.clear();
		this->edge_msg_read_buffer.resize(this->edge_msg_buffer_len * singel_edge_total_info_size);
		if (!reader->fread(edge_msg_read_buffer.data(), this->edge_msg_buffer_len * singel_edge_total_info_size,
			this->edge_msg_now_pos * singel_edge_total_info_size))
		{
			std::cout << "read fail msg" << std::endl;
			++*(int *)NULL;
		}
	}
	// 得到一个文件的第一条边的信息,如果文件是空的就返回false
	bool SSTableParser::GetFirstEdge()
	{
		if (!this->edge_cnt) {
			return false;
		}
		this->edge_allocation_now_pos = 0;
		this->edge_msg_now_pos = 0;
		if (this->edge_msg_now_pos != this->edge_cnt) {
			this->ReadEdgeAllocationBuffer();
			this->ReadEdgeMsgBuffer();
		}
		if (!this->GetNextEdge()) {
			return false;
		}
		return true;
	}
	bool SSTableParser::GetNextEdge()
	{
		if (this->edge_msg_now_pos >= this->edge_cnt) {
			return false;
		}
		while (true) {
			// 读edge_allocation数组，直到当前读到的边的位置在这个分配数组范围之内结束
			if (this->edge_allocation_buffer_pos >= this->edge_allocation_buffer_len) {
				this->ReadEdgeAllocationBuffer();
			}
			if (util::GetDecodeFixed<edge_num_t>(this->edge_allocation_read_buffer.data() + this->edge_allocation_buffer_pos * sizeof(edge_num_t)) > this->edge_msg_now_pos) {
				break;
			}
			this->edge_allocation_buffer_pos++;
			this->edge_allocation_now_pos++;
		}
		this->now_edge_src = this->edge_allocation_now_pos + this->src_b;
		if (this->edge_msg_buffer_pos >= this->edge_msg_buffer_len) {
			this->ReadEdgeMsgBuffer();
		}
		auto offset = this->edge_msg_buffer_pos * singel_edge_total_info_size;
		this->now_edge_dst = util::GetDecodeFixed<vertex_t>(this->edge_msg_read_buffer.data() + offset);
		this->now_edge_prop = util::GetDecodeFixed<edge_property_t>(this->edge_msg_read_buffer.data() + offset + sizeof(vertex_t));
		this->edge_msg_now_pos++;
		this->edge_msg_buffer_pos++;
		return true;
	}
}