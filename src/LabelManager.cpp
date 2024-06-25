#include "BACH/label/LabelManager.h"

namespace BACH
{
	LabelManager::LabelManager() :
		VertexLabelIdIndex(),
		EdgeLabelIdIndex(),
		VertexLabel(),
		EdgeLabel(),
		vertex_label_num(0),
		edge_label_num(0)
	{

	}
	label_t LabelManager::AddVertexLabel(std::string_view label)
	{
		VertexLabelIdIndex.insert(std::make_pair(label, vertex_label_num++));
		VertexLabel.emplace_back(label);
		return vertex_label_num;
	}

	std::tuple<label_t, label_t, label_t> LabelManager::AddEdgeLabel(std::string_view label,
		std::string_view src_label, std::string_view dst_label)
	{
		label_t src_id = GetVertexLabelId(src_label);
		label_t dst_id = GetVertexLabelId(dst_label);
		EdgeLabelIdIndex[label] = edge_label_num++;
		EdgeLabel.emplace_back(label, src_id, dst_id);
		return std::make_tuple(edge_label_num - 1, src_id, dst_id);
	}

	label_t LabelManager::GetVertexLabelId(std::string_view label)
	{
		auto label_iter = VertexLabelIdIndex.find(label);
		if (label_iter == VertexLabelIdIndex.end())
		{
			//todo:error
			return -1;
		}
		return label_iter->second;
	}
	label_t LabelManager::GetEdgeLabelId(std::string_view label)
	{
		auto label_iter = EdgeLabelIdIndex.find(label);
		if (label_iter == EdgeLabelIdIndex.end())
		{
			//todo:error
			return -1;
		}
		return label_iter->second;
	}
	std::string_view LabelManager::GetVertexLabel(label_t id)
	{
		return VertexLabel[id];
	}
	std::string_view LabelManager::GetEdgeLabel(label_t id)
	{
		return EdgeLabel[id].label;
	}
}