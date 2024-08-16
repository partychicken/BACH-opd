#include "BACH/label/LabelManager.h"

namespace BACH
{
	LabelManager::LabelManager() :
		VertexLabelIdIndex(),
		EdgeLabelIdIndex(),
		VertexLabel(),
		EdgeLabel() {}

	label_t LabelManager::AddVertexLabel(std::string label_name)
	{
		VertexLabelIdIndex.insert(std::make_pair(label_name, vertex_label_num));
		VertexLabel.emplace_back(label_name);
		return vertex_label_num++;
	}

	std::tuple<label_t, label_t, label_t>  LabelManager::AddEdgeLabel(
		std::string edge_label_name,
		std::string src_label_name, std::string dst_label_name)
	{
		label_t src_id = GetVertexLabelId(src_label_name);
		label_t dst_id = GetVertexLabelId(dst_label_name);
		EdgeLabelIdIndex[edge_label_name] = edge_label_num;
		EdgeLabel.emplace_back(edge_label_name, src_id, dst_id);
		return std::make_tuple(edge_label_num++, src_id, dst_id);
	}

	label_t LabelManager::GetVertexLabelId(std::string label)
	{
		auto label_iter = VertexLabelIdIndex.find(label);
		if (label_iter == VertexLabelIdIndex.end())
		{
			return -1;
		}
		return label_iter->second;
	}

	label_t LabelManager::GetSrcVertexLabelId(label_t id)
	{
		return std::get<1>(EdgeLabel[id]);
	}

	label_t LabelManager::GetEdgeLabelId(std::string label)
	{
		auto label_iter = EdgeLabelIdIndex.find(label);
		if (label_iter == EdgeLabelIdIndex.end())
		{
			return -1;
		}
		return label_iter->second;
	}

	std::string_view LabelManager::GetVertexLabel(label_t id)
	{
		if (id >= vertex_label_num)
			return "";
		return VertexLabel[id];
	}

	std::string_view LabelManager::GetEdgeLabel(label_t id)
	{
		if (id >= edge_label_num)
			return "";
		return std::get<0>(EdgeLabel[id]);
	}
}