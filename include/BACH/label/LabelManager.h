#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>
#include "BACH/utils/types.h"

namespace BACH
{
	class Transaction;
	class LabelManager
	{
	public:
		LabelManager();
		LabelManager(const LabelManager&) = delete;
		LabelManager& operator=(const LabelManager&) = delete;
		~LabelManager() = default;

		label_t AddVertexLabel(std::string_view label_name);
		std::tuple<label_t, label_t, label_t>  AddEdgeLabel(
			std::string_view edge_label_name,
			std::string_view src_label_name, std::string_view dst_label_name);
		label_t GetVertexLabelId(std::string_view label);
		label_t GetEdgeLabelId(std::string_view label);
		std::string_view GetVertexLabel(label_t id);
		std::string_view GetEdgeLabel(label_t id);

	private:
		std::unordered_map <std::string_view, label_t> VertexLabelIdIndex;
		std::unordered_map <std::string_view, label_t> EdgeLabelIdIndex;
		std::vector <std::string> VertexLabel;
		std::vector <std::tuple<std::string,label_t,label_t>> EdgeLabel;
		label_t vertex_label_num;
		label_t edge_label_num;
		friend class Transaction;
		friend class DB;
	};
}
