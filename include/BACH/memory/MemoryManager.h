#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include "VertexLabelIdEntry.h"
#include "VertexEntry.h"
#include "BACH/db/DB.h"
#include "BACH/label/LabelManager.h"
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

		void MemTablePersistence(label_t label_id,
			std::shared_ptr<SizeEntry> size_info, time_t now_time);
		void PersistenceAll();
		void VertexPropertyPersistence(label_t label_id);
		vertex_t GetVertexNum(label_t label_id, time_t now_time) const;

	private:

		//a struct to store memtable
		//tbb::concurrent_vector <EdgeLabelIdEntry> EdgeLabelIndex;
		
		//concurrent? other struct?
		/*tbb::concurrent_*/ std::vector <VertexLabelIdEntry> VertexLabelIndex;
		DB* db;
		friend class FileManager;
		friend class Transaction;
	};

}