#include "BACH/sstable/SSTableParser.h"

namespace BACH
{
	SSTableParser::SSTableParser(DB* _db, label_t _label,
		std::shared_ptr<FileReader> _fileReader,
		std::shared_ptr<Options> _options, bool if_read_filter) :
		db(_db),
		label(_label),
		reader(_fileReader),
		options(_options),
		read_filter(if_read_filter)
	{
        file_size = (*reader).file_size();

        //SSTable用CSR块存储信息，src_vertex->dst_vertex
		size_t offset = sizeof(vertex_t) * 2 + sizeof(edge_num_t) + sizeof(vertex_num_t);
		char infobuf[offset];
		if (!reader->rread(infobuf, offset, offset))
		{
			std::cout << "read fail" << std::endl;
		}
		util::DecodeFixed(infobuf, src_b);
		util::DecodeFixed(infobuf + sizeof(vertex_t), src_e);
		util::DecodeFixed(infobuf + sizeof(vertex_t) * 2, edge_cnt);
        // SStable最后部分是三个参数，其中src_b和src_e代表当前文件src_vertex的起始和终止的vertex_id的范围;edge_cnt代表边的总数量;

		edge_msg_end_pos = (sizeof(vertex_t) + sizeof(edge_property_t)) * edge_cnt;
        // 第一部分是边的信息，每条边包括dst_vertex_id和这条边代表的边属性，边属性暂定为定长的double类型

		edge_allocation_end_pos = edge_msg_end_pos + sizeof(edge_num_t) * (src_b-src_e+1);
        // 第二部分是记录src对应的dst在边数组中的位置范围，例如src为10对应的dst范围为( edge_list[src_info_list[src-1]],edge_list[src_info_list[src]] ]

        filter_allocation_end_pos = file_size - size_t(vertex_t) * 2 - size_t(edge_num_t);
        // 第四部分是布隆过滤器的分配数组，长度为固定长度，即代表文件中能表示的src_vertex_id的范围，然后每个字段为（filter_len，hash_func_num）,就可以计算出布隆过滤器的数组长度 

		filter_end_pos = filter_allocation_end_pos -  (sizeof(size_t) + sizeof(idx_t)) * (src_e - src_b + 1);
        // 第三部分是布隆过滤器的信息，每一个src分配一个布隆过滤器，布隆过滤器不是定长的，因此需要第四部分的过滤器分配数组来标识src对应的布隆过滤器,并且由filter_len来标识过滤器长度
    
	}
    // 在此文件中查找特定src->dst的边，如果不存在则返回null，存在返回一个指向(vertex_id,edge_property)的指针
	std::shared_ptr<std::pair<vertex_t,edge_property_t>> SSTableParser::FindEdge(
		vertex_t src, vertex_t dst)
	{

        // 通过布隆过滤器分配数组，拿到布隆过滤器date的存放位置（存放位置是一个区间，因此需要取布隆过滤器分配数组的src-1以及src的信息）以及布隆过滤器的func_num
        size_t offset,len;
        idx_t func_num;
        if (src == this->src_b) 
        {
            char filter_allocation_info[singel_filter_allocation_size];
            if(!reader->fread(filter_allocation_info,singel_filter_allocation_size,filter_end_pos))
            {
               std::cout << "read fail" << std::endl; 
            }
            offset = 0;
            len = util::GetDecodeFixed<size_t>(filter_allocation_info);
            func_num = util::GetDecodeFixed<idx_t>(filter_allocation_info + sizeof(size_t));
        }
        else {
            char filter_allocation_info[singel_filter_allocation_size * 2];
            if(!reader->fread(filter_allocation_info,singel_filter_allocation_size * 2,filter_end_pos + (src - src_b - 1) * singel_filter_allocation_size))
            {
               std::cout << "read fail" << std::endl;
            }
            offset = util::GetDecodeFixed<size_t>(filter_allocation_info) + 1;
            len = util::GetDecodeFixed<size_t>(filter_allocation_info + singel_filter_allocation_size) - offset + 1;
            func_num = util::GetDecodeFixed<idx_t>(filter_allocation_info + singel_filter_allocation_size + sizeof(size_t));
        }

        // query 如果布隆过滤器长度为0直接证明没有src的边
        if(!len)
        {
            return NULL;
        }

        // 得到布隆过滤器的位置和长度之后在存放布隆过滤器信息的数组中拿到相应的布隆过滤器data
		auto filter = std::make_shared<BloomFilter>();
		std::string filter_data;
		filter_data.resize(len);
		if (!reader->fread(filter_data.data(), len, offset + edge_allocation_end_pos)) {
            std::cout << "read fail" << std::endl; 
        }
		filter->create_from_data(func_num, filter_data);
		if (!filter->exists(src, dst))
			return NULL;
		
		GetEdgeRangeBySrcId(src);
		
        // 批量读边，检查是否存在src->dst这条边
        auto singel_read_max_len = this->options->READ_BUFFER_SIZE / singel_edge_total_info_size * singel_edge_total_info_size;
        auto read_num = (this->src_edge_len - 1) / singel_read_max_len + 1;
        for(auto i = 1; i <= read_num; i++)
        {   
            size_t len = (i != read_num) ? singel_read_max_len : this->src_edge_len % singel_read_max_len;
            char buffer[len];
            if(!reader->fread(buffer,this->src_edge_info_offset,len)) {
                std::cout << "read fail" << std::endl;  
            }
            size_t offset = 0;
            this->src_edge_info_offset += len;
            for(auto j = 0; j < len / singel_edge_total_info_size; j++)
            {
                auto vertex_id = util::GetDecodeFixed<vertex_t>(buffer + offset);
                if (vertex_id == dst) {
                    return std::make_shared<std::pair<vertex_t,edge_property_t>>(dst,util::GetDecodeFixed<edge_property_t>(buffer + offset + sizeof(vertex_t)));
                }
                offset += singel_edge_total_info_size;   
            }
        }
		
		return NULL;
	}
    // 在一个文件中批量查找以src为起点的所有边，边属性的条件过滤函数以参数形式放入，过滤之后的边信息放在answer指向的vector中
	void SSTableParser::FindEdges(vertex_t src, bool (*func)(edge_property_t),
		std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>> answer)
	{
		GetSrc(src);
		if(!this->src_edge_len)
        {
            return ;
        }

        // 批量读边，将边属性过滤后的边放入answer中
        auto singel_read_max_len = this->options->READ_BUFFER_SIZE / singel_edge_total_info_size * singel_edge_total_info_size;
        auto read_num = (this->src_edge_len - 1) / singel_read_max_len + 1;
        for(auto i = 1; i <= read_num; i++)
        {   
            auto len = (i != read_num) ? singel_read_max_len : this->src_edge_len % singel_read_max_len;
            char buffer[len];
            if(!reader->fread(buffer,this->src_edge_info_offset,len)) {
                std::cout << "read fail" << std::endl;  
            }
            size_t offset = 0;
            this->src_edge_info_offset += len;
            for(auto j = 0; j < len / singel_edge_total_info_size; j++)
            {
                auto edge_property = util::GetDecodeFixed<edge_property_t>(buffer + offset + sizeof(vertex_t));
                if(func(edge_property)){
                    auto vertex_id = util::GetDecodeFixed<vertex_t>(buffer + offset);
                    (*answer).push_back(std::pair<vertex_t,edge_property_t>(vertex_id,edge_property));
                }
                offset += singel_edge_total_info_size;   
            }
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
               std::cout << "read fail" << std::endl; 
            }
			this->src_edge_info_offset = 0;
            this->src_edge_len = util::GetDecodeFixed<edge_num_t>(buffer) * singel_edge_total_info_size;
		}
		else
		{
			size_t offset = this->edge_msg_end_pos + (src - this->src_b - 1) * singel_edge_total_info_size;
			size_t len = sizeof(edge_num_t) * 2;
			char buffer[len];
            if (!reader->fread(buffer, len, offset))
            {
               std::cout << "read fail" << std::endl; 
            
            }
            this->src_edge_info_offset = util::GetDecodeFixed<edge_num_t>(buffer) * singel_edge_total_info_size;
			this->src_edge_len = (util::GetDecodeFixed<edge_num_t>(buffer+sizeof(edge_num_t)) - util::GetDecodeFixed<edge_num_t>(buffer)) * singel_edge_total_info_size;
		}
	}
	void SSTableParser::BatchReadEdge()
	{
		this->edge_buffer = std::make_shared<Buffer<EdgeEntry, edge_t>>(
			reader, 0, this->edge_msg_end_pos, options, sizeof(edge_t) + sizeof(edge_property_t));
	}
	vertex_t SSTableParser::GetNowSrc()const
	{
		return this->now_src_p + this->src_b;
	}
	edge_t SSTableParser::GetNowVerCount()const
	{
		vertex_t now_index = dst_buffer->GetNowIndex();
		if (now_index == src_index[now_src_p])
		{
			return 0;
		}
		return dst_buffer->GetNow().ver_index - last_ver_index;
	}
	std::shared_ptr<EdgeEntry> SSTableParser::GetNowEdge(bool force)const
	{
		if (!force)
		{
			edge_t now_index = edge_buffer->GetNowIndex();
			if (now_index == dst_buffer->GetNow().ver_index)
			{
				return NULL;
			}
		}
		return std::make_shared<EdgeEntry>(edge_buffer->GetNow());
	}
	void SSTableParser::Reset(bool reset_dst)
	{
		now_src_p = 0;
		if (reset_dst)
		{
			dst_buffer->Reset();
			last_ver_index = 0;
		}
	}
	void SSTableParser::NextSrc()
	{
		++now_src_p;
		if (now_src_p >= src_index.size())
		{
			now_src_p = MAXVERTEX - src_e;//return -1 when GetNowSrc()
			return;
		}
		NextDst();
	}
	bool SSTableParser::NextDst()
	{
		edge_t now_index = dst_buffer->GetNowIndex();
		if (now_index == src_index[now_src_p] - 1)
		{
			return false;
		}
		last_ver_index = dst_buffer->GetNow().ver_index;
		dst_buffer->Next();
		NextEdge();
		return true;
	}
	bool SSTableParser::NextEdge(bool force)
	{
		edge_t now_index = edge_buffer->GetNowIndex();
		if (!force)
		{
			if (now_index == dst_buffer->GetNow().ver_index - 1)
			{
				return false;
			}
		}
		if (now_index < edge_cnt - 1)
		{
			edge_buffer->Next();
			return true;
		}
		edge_buffer->Next();
		return false;
	}
}