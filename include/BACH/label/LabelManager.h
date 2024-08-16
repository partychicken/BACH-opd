#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>
#include "BACH/utils/types.h"

namespace BACH
{
	class LabelManager
	{
	public:
		LabelManager();
		LabelManager(const LabelManager&) = delete;
		LabelManager& operator=(const LabelManager&) = delete;
		~LabelManager() = default;

		label_t AddVertexLabel(std::string label_name);
		std::tuple<label_t, label_t, label_t>  AddEdgeLabel(
			std::string edge_label_name,
			std::string src_label_name, std::string dst_label_name);
		label_t GetVertexLabelId(std::string label);
		label_t GetSrcVertexLabelId(label_t id);
		label_t GetEdgeLabelId(std::string label);
		std::string_view GetVertexLabel(label_t id);
		std::string_view GetEdgeLabel(label_t id);

	private:
		std::unordered_map <std::string, label_t> VertexLabelIdIndex;
		std::unordered_map <std::string, label_t> EdgeLabelIdIndex;
		std::vector <std::string> VertexLabel;
		std::vector <std::tuple<std::string, label_t, label_t>> EdgeLabel;
		label_t vertex_label_num = 0;
		label_t edge_label_num = 0;
		friend class Transaction;
	};
}
