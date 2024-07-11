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
		vertex_t GetVertexNum(label_t label_id) const;
		void DelVertex(vertex_t vertex, label_t label, time_t now_time);

		void PutEdge(vertex_t src, vertex_t dst, label_t label,
			edge_property_t property, time_t now_time);
		void DelEdge(vertex_t src, vertex_t dst, label_t label, time_t now_time);
		edge_property_t GetEdge(vertex_t src, vertex_t dst, label_t label,
			time_t now_time);
		void GetEdges(vertex_t src, label_t label, time_t now_time,
			std::shared_ptr<std::vector
			<std::tuple<vertex_t, vertex_t, edge_property_t>>> answer,
			bool (*func)(edge_property_t));

		time_t GetVertexDelTime(label_t edge_label_id,vertex_t src) const;
		void ClearDelTable(time_t time);
		VersionEdit* MemTablePersistence(label_t label_id, SizeEntry* size_info);
		//void PersistenceAll();

	private:
		ConcurrentArray <EdgeLabelEntry*> EdgeLabelIndex;
		ConcurrentArray <VertexLabelEntry*> VertexLabelIndex;
		std::shared_mutex MemTableDelTableMutex;
		std::map<time_t, std::vector<SizeEntry*>> MemTableDelTable;
		DB* db;
		

		void vertex_property_persistence(label_t label_id);
		void immute_memtable(SizeEntry*& size_info, label_t label);
		edge_t find_edge(vertex_t src, vertex_t dst, label_t label_id);

		//friend class FileManager;
		//friend class LabelManager;
		//friend class Transaction;
	};

}