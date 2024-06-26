#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>
#include "BACH/utils/types.h"

namespace BACH
{
	class Transaction;
	struct EdgeLabelEntry
	{
		std::string label;
		label_t src_label_id;
		label_t dst_label_id;

		EdgeLabelEntry(std::string_view _label, label_t src, label_t dst) :
			label(_label), src_label_id(src), dst_label_id(dst) {}
	};
	class LabelManager
	{
	public:
		LabelManager();
		LabelManager(const LabelManager&) = delete;
		LabelManager& operator=(const LabelManager&) = delete;
		~LabelManager() = default;

		label_t AddVertexLabel(std::string_view label_name);
		label_t AddEdgeLabel(std::string_view edge_label_name,
			std::string_view src_label_name, std::string_view dst_label_name);
		label_t AddEdgeLabel(std::string_view edge_label_name,
			label_t src_label, label_t dst_label);
		label_t GetVertexLabelId(std::string_view label);
		label_t GetEdgeLabelId(std::string_view label);
		std::string_view GetVertexLabel(label_t id);
		std::string_view GetEdgeLabel(label_t id);

	private:
		std::unordered_map <std::string_view, label_t> VertexLabelIdIndex;
		std::unordered_map <std::string_view, label_t> EdgeLabelIdIndex;
		std::vector <std::string> VertexLabel;
		std::vector <EdgeLabelEntry> EdgeLabel;
		label_t vertex_label_num;
		label_t edge_label_num;
		friend class Transaction;
		friend class DB;
	};
}
