#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <sul/dynamic_bitset.hpp>
#include "VertexLabelIdEntry.h"
#include "VertexEntry.h"
#include "BACH/label/LabelManager.h"
#include "BACH/property/PropertyFileBuilder.h"
#include "BACH/property/PropertyFileParser.h"
#include "BACH/sstable/SSTableBuilder.h"
#include "BACH/sstable/Version.h"
#include "BACH/utils/types.h"
#include "BACH/utils/Options.h"


namespace BACH
{
	class DB;
	class MemoryManager
	{
	public:
		MemoryManager() = delete;
		MemoryManager(const MemoryManager&) = delete;
		MemoryManager& operator=(const MemoryManager&) = delete;
		MemoryManager(DB* _db);
		~MemoryManager() = default;

		void AddVertexLabel();
		void AddEdgeLabel(label_t src_label, label_t dst_label);

		vertex_t AddVertex(label_t label_id);
		void PutVertex(label_t label_id, vertex_t vertex_id,
			std::string_view property);
		std::shared_ptr<std::string> GetVertex(vertex_t vertex, label_t label,
			time_t now_time);
		vertex_t GetVertexNum(label_t label_id) const;
		void DelVertex(vertex_t vertex, label_t label, time_t now_time);

		void PutEdge(vertex_t src, vertex_t dst, label_t label,
			edge_property_t property, time_t now_time);
		void DelEdge(vertex_t src, vertex_t dst, label_t label, time_t now_time);
		edge_property_t GetEdge(vertex_t src, vertex_t dst, label_t label,
			time_t now_time);
		void GetEdges(vertex_t src, label_t label, time_t now_time,
			std::shared_ptr<std::vector
			<std::pair<vertex_t, edge_property_t>>> answer_temp[3],
			vertex_t& c,
			const std::function<bool(edge_property_t&)>& func);
		void EdgeLabelScan(label_t label,
			const std::function<void(vertex_t&, vertex_t&, edge_property_t&)>& func);

		size_t GetMergeType(label_t label_id, vertex_t src_b, idx_t level);
		time_t GetVertexDelTime(label_t edge_label_id, vertex_t src) const;
		VersionEdit* MemTablePersistence(label_t label_id,
			idx_t file_id, std::shared_ptr < SizeEntry > size_info);
		void PersistenceAll();

		void merge_answer(std::shared_ptr<std::vector
			<std::pair<vertex_t, edge_property_t>>> answer_temp[3],
			vertex_t& c);

	private:
		ConcurrentArray <EdgeLabelEntry*> EdgeLabelIndex;
		ConcurrentArray <VertexLabelEntry*> VertexLabelIndex;
		DB* db;

		void vertex_property_persistence(label_t label_id);
		void immute_memtable(std::shared_ptr<SizeEntry> size_info, label_t label);
		edge_t find_edge(vertex_t src, vertex_t dst, std::shared_ptr < SizeEntry > entry);
	};
}