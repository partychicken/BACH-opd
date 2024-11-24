#include "BACH/sstable/SSTableBuilder.h"
#include "BACH/db/DB.h"

namespace BACH
{
	SSTableBuilder::SSTableBuilder(std::shared_ptr<FileWriter> _fileWriter, std::shared_ptr<Options> _options) :
		writer(_fileWriter),
		options(_options){}
	//void SSTableBuilder::AddFilter(idx_t keys_num, double false_positive)
	//{
	//	filter.push_back(std::make_shared<BloomFilter>(keys_num, false_positive));
	//}
	void SSTableBuilder::SetSrcRange(vertex_t src_b, vertex_t src_e)
	{
		this->src_b = src_b;
		this->src_e = src_e;
	}
	void SSTableBuilder::AddEdge(vertex_t index, vertex_t dst, edge_property_t edge_property)
	{
		std::string temp_data;
		util::PutFixed(temp_data, dst);
		util::PutFixed(temp_data, edge_property);
		writer->append(temp_data.data(), temp_data.size());
		//this->filter[index]->insert(dst);
		//this->edge_dst_id_list.push_back(dst);
		this->src_edge_num++;
	}
	void SSTableBuilder::ArrangeCurrentSrcInfo()
	{
		edge_allocation_list.push_back(this->src_edge_num);
		this->src_edge_num = 0;
		//this->AddFilter(this->edge_dst_id_list.size(), this->options->FALSE_POSITIVE);
		//for (auto dst_id : this->edge_dst_id_list)
		//{
		//	this->filter.back()->insert(dst_id);
		//}
		//this->edge_dst_id_list.clear();
	}
	sul::dynamic_bitset<>* SSTableBuilder::ArrangeSSTableInfo()
	{
		auto bitmap = new sul::dynamic_bitset<>();
		edge_num_t edge_num_prefix_sum = 0;
		for (auto num : this->edge_allocation_list)
		{
			edge_num_prefix_sum += num;
			writer->append((char*)(&edge_num_prefix_sum), sizeof(edge_num_t));
			if (num == 0)
				bitmap->push_back(0);
			else
				bitmap->push_back(1);
		}
		//for (auto& i : this->filter)
		//{
		//	writer->append(i->data().data(), i->data().size());
		//}
		//size_t filter_len_prefix_sum = 0;
		std::string metadata;
		//for (auto& i : this->filter)
		//{
		//	filter_len_prefix_sum += i->data().size();
		//	util::PutFixed(metadata, filter_len_prefix_sum);
		//	util::PutFixed(metadata, (idx_t)i->get_func_num());
		//}
		util::PutFixed(metadata, src_b);
		util::PutFixed(metadata, src_e);
		util::PutFixed(metadata, edge_num_prefix_sum);
		writer->append(metadata.data(), metadata.size());
		writer->flush();
		return bitmap;
	}
}