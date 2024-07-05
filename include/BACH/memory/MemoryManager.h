#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include "VertexLabelIdEntry.h"
#include "VertexEntry.h"
#include "BACH/db/DB.h"
#include "BACH/label/LabelManager.h"
#include "BACH/property/PropertyFileBuilder.h"
#include "BACH/property/PropertyFileParser.h"
#include "BACH/utils/types.h"
#include "BACH/utils/options.h"


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

		vertex_t AddVertex(label_t label_id, std::string_view property,
			time_t now_time);
		std::shared_ptr<std::string> GetVertex(vertex_t vertex, label_t label,
			time_t now_time);
		void DelVertex(vertex_t vertex, label_t label, time_t now_time);

		void PutEdge(vertex_t src, vertex_t dst, label_t label,
			edge_property_t property, time_t now_time);
		void DelEdge(vertex_t src, vertex_t dst, label_t label, time_t now_time);
		edge_property_t GetEdge(vertex_t src, vertex_t dst, label_t label,
			time_t now_time);

		void MemTablePersistence(label_t label_id,
			std::shared_ptr<SizeEntry> size_info, time_t now_time);
		void PersistenceAll();
		vertex_t GetVertexNum(label_t label_id, time_t now_time) const;

	private:
		ConcurrentArray <std::shared_ptr <EdgeLabelEntry>> EdgeLabelIndex;
		ConcurrentArray <std::shared_ptr <VertexLabelEntry>> VertexLabelIndex;
		DB* db;

		void vertex_property_persistence(label_t label_id);
		edge_t find_edge(vertex_t src, vertex_t dst, label_t label_id);

		friend class FileManager;
		friend class LabelManager;
		friend class Transaction;
	};

}