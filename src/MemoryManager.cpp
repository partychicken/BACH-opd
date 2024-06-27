#include "BACH/memory/MemoryManager.h"

namespace BACH
{
	void MemoryManager::AddVertexLabel()
	{
		VertexLabelIndex.push_back(std::make_shared<VertexLabelEntry>());
		for (label_t i = 0; i < EdgeLabelIndex.size(); ++i)
		{
			if (EdgeLabelIndex[i]->src_label_id == VertexLabelIndex.size() - 1)
			{
				EdgeLabelIndex[i]->VertexIndex.push_back(
					std::make_shared<VertexEntry>(
						EdgeLabelIndex[i]->now_size_info));
			}
		}
	}
	void MemoryManager::AddEdgeLabel(label_t src_label, label_t dst_label)
	{
		EdgeLabelIndex.push_back(std::make_shared<EdgeLabelEntry>());
	}
}