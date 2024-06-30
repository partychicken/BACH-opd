#include "BACH/memory/MemoryManager.h"

namespace BACH
{
	void MemoryManager::AddVertexLabel()
	{
		VertexLabelIndex.push_back(std::make_shared<VertexLabelEntry>());
	}
	void MemoryManager::AddEdgeLabel(label_t src_label, label_t dst_label)
	{
		EdgeLabelIndex.push_back(std::make_shared<EdgeLabelEntry>(src_label));
	}

	vertex_t MemoryManager::AddVertex(label_t label_id,
		std::string_view property, time_t now_time)
	{
		std::unique_lock<std::shared_mutex> lock(VertexLabelIndex[label_id]->mutex);
		vertex_t vertex_id = VertexLabelIndex[label_id]->total_vertex++;
		VertexLabelIndex[label_id]->VertexProperty.push_back(
			static_cast<std::string>(property));
		for (label_t i = 0; i < EdgeLabelIndex.size(); ++i)
		{
			if (EdgeLabelIndex[i]->src_label_id == label_id)
			{
				std::unique_lock<std::shared_mutex> lock(EdgeLabelIndex[i]->mutex);
				if (EdgeLabelIndex[i]->now_size_info->size >= db->options->MERGE_NUM)
				{
					EdgeLabelIndex[i]->now_size_info = std::make_shared<SizeEntry>();
				}
				EdgeLabelIndex[i]->VertexIndex.push_back(
					std::make_shared<VertexEntry>(EdgeLabelIndex[i]->now_size_info));
			}
		}
		VertexLabelIndex[label_id]->property_size += property.size();
		if (VertexLabelIndex[label_id]->property_size >
			db->options->VERTEX_PROPERTY_MAX_SIZE)
		{
			vertex_property_persistence(label_id);
		}
		lock.unlock();
	}
	std::shared_ptr<std::string> MemoryManager::FindVertex(vertex_t vertex, label_t label,
		time_t now_time)
	{
		auto x = VertexLabelIndex[label]->deletetime.find(vertex);
		if (x == VertexLabelIndex[label]->deletetime.end()
			|| x->second > now_time)
		{
			if (vertex >= VertexLabelIndex[label]->unpersistence)
			{
				return std::make_shared<std::string>(
					VertexLabelIndex[label]->VertexProperty[vertex]);
			}
			else
			{
				idx_t file_no = VertexLabelIndex[label]->FileIndex.lowerbound(vertex);
				std::string file_name = db->options->STORAGE_DIR + "/"
					+ static_cast<std::string>(db->Labels->GetVertexLabel(label)) + "_"
					+ std::to_string(file_no) + ".property";
				auto parser = std::make_shared<PropertyFileParser>(
					std::make_shared<FileReader>(file_name));
				return parser->GetProperty(vertex - file_no);
			}
		}
		else
		{
			return NULL;
		}
	}
	void MemoryManager::DelVertex(vertex_t vertex, label_t label, time_t now_time)
	{
		VertexLabelIndex[label]->deletetime[vertex] = now_time;
		for (label_t i = 0; i < EdgeLabelIndex.size(); ++i)
		{
			if (EdgeLabelIndex[i]->src_label_id == label)
			{
				std::unique_lock<std::shared_mutex> label_lock(EdgeLabelIndex[i]->mutex);
				std::unique_lock<std::shared_mutex> v_lock(
					EdgeLabelIndex[label]->VertexIndex[vertex]->mutex);
				EdgeLabelIndex[i]->VertexIndex[vertex]->deadtime = now_time;
			}
		}
	}


	void MemoryManager::vertex_property_persistence(label_t label_id)
	{
		std::string file_name = db->options->STORAGE_DIR + "/"
			+ static_cast<std::string>(db->Labels->GetVertexLabel(label_id)) + "_"
			+ std::to_string(VertexLabelIndex[label_id]->unpersistence) + ".property";
		auto fw = std::make_shared<FileWriter>(file_name, false);
		auto pf = std::make_shared<PropertyFileBuilder>(fw);
		for (auto& i : VertexLabelIndex[label_id]->VertexProperty)
		{
			pf->AddProperty(i);
		}
		pf->FinishFile();
		VertexLabelIndex[label_id]->property_size = 0;
		VertexLabelIndex[label_id]->FileIndex.push_back(
			VertexLabelIndex[label_id]->unpersistence);
		VertexLabelIndex[label_id]->unpersistence +=
			VertexLabelIndex[label_id]->VertexProperty.size();
		VertexLabelIndex[label_id]->VertexProperty.clear();
	}
}