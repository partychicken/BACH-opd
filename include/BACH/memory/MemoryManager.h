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
#include "BACH/sstable/SSTableBuilder.h"
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

		void ClearDelTable(time_t time);
		VersionEdit* MemTablePersistence(label_t label_id,
			SizeEntry* size_info, time_t now_time, Version* basic_version);
		void PersistenceAll();
		vertex_t GetVertexNum(label_t label_id, time_t now_time) const;

	private:
		ConcurrentArray <EdgeLabelEntry*> EdgeLabelIndex;
		ConcurrentArray <VertexLabelEntry*> VertexLabelIndex;
		std::shared_mutex MemTableDelTableMutex;
		std::map<time_t, std::vector<SizeEntry*>> MemTableDelTable;
		DB* db;

		void vertex_property_persistence(label_t label_id);
		edge_t find_edge(vertex_t src, vertex_t dst, label_t label_id);

		//friend class FileManager;
		//friend class LabelManager;
		//friend class Transaction;
	};

}