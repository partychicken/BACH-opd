#pragma once

#include <set>
#include <tbb/concurrent_set.h>
#include "Transaction.h"
#include "BACH/label/LabelManager.h"
#include "BACH/memory/MemoryManager.h"
#include "BACH/sstable/FileManager.h"

namespace BACH
{
	class LabelManager;
	class FileManager;
	class MemoryManager;
	class SSTableParser;
	class DB
	{
	public:
		DB() = delete;
		DB(const DB&) = delete;
		DB& operator=(const DB&) = delete;
		~DB();
		DB(std::shared_ptr<Options> _options);
		Transaction BeginTransaction();
		Transaction BeginReadOnlyTransaction();
		//return the id of new vlabel
		label_t AddVertexLabel(std::string_view label_name);
		//return the id of new elabel
		label_t AddEdgeLabel(std::string_view edge_label_name,
			std::string_view src_label_name, std::string_view dst_label_name);
		//compaction loop for background thread
		void CompactLoop();
		//compact all edge
		void CompactAll();

		//void Persistence(std::string_view label, vertex_t merge_id);
		//void TestMerge(Compaction& x, idx_t type);
	private:
		std::shared_ptr<Options> options;
		std::atomic<time_t> epoch_id;
		std::shared_mutex epoch_table_mutex;
		std::set<time_t> epoch_table;
		std::vector<std::shared_ptr<std::thread>> compact_thread;
		std::unique_ptr<LabelManager> Labels = NULL;
		std::unique_ptr<MemoryManager> Memtable = NULL;
		std::unique_ptr<FileManager> Files = NULL;
		bool close = false;

		friend class Transaction;
		friend class MemoryManager;
		friend class FileManager;
		friend class LabelManager;
		friend class SSTableParser;
	};
}