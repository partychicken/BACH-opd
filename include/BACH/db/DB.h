#pragma once

#include <set>
#include <sys/resource.h>
#include "BACH/file/FileReaderCache.h"
#include "BACH/label/LabelManager.h"
#include "BACH/memory/MemoryManager.h"
#include "BACH/sstable/FileManager.h"
#include "BACH/sstable/Version.h"
#include "BACH/sstable/RelVersion.h"
#include "BACH/utils/ConcurrentList.h"
#include "BACH/memory/RowMemoryManager.h"

namespace BACH
{
	class Transaction;
	class DB
	{
	public:
		DB() = delete;
		DB(const DB&) = delete;
		DB& operator=(const DB&) = delete;
		~DB();
		DB(std::shared_ptr<Options> _options);
		DB(std::shared_ptr<Options> _options, idx_t column_num);
		Transaction BeginTransaction();
		Transaction BeginReadOnlyTransaction();

		Transaction BeginRelTransaction();
		Transaction BeginReadOnlyRelTransaction();


		//return the id of new vlabel
		label_t AddVertexLabel(std::string label_name);
		//return the id of new elabel
		label_t AddEdgeLabel(std::string edge_label_name,
			std::string src_label_name, std::string dst_label_name);
		//compact all edge
		void CompactAll(double_t ratio = 0.0);
		void ProgressVersion(VersionEdit* edit, time_t time,
			std::shared_ptr<SizeEntry> size = NULL, bool force_level = false);

		//void Persistence(std::string_view label, vertex_t merge_id);
		//void TestMerge(Compaction& x, idx_t type);

		std::shared_ptr<Options> options;
		std::unique_ptr<LabelManager> Labels;
		std::unique_ptr<MemoryManager> Memtable;
		std::unique_ptr<FileManager> Files;
		std::unique_ptr<FileReaderCache> ReaderCaches;

		std::unique_ptr<rowMemoryManager> RowMemtable;


	private:
		std::atomic<time_t> epoch_id;
		ConcurrentList<time_t> write_epoch_table;
		std::mutex version_mutex;
		std::atomic<Version *> read_version = NULL;
		Version * current_version = NULL;

		std::atomic<RelVersion*> read_rel_version = NULL;
		RelVersion* current_rel_version = NULL;

		std::vector<std::shared_ptr<std::thread>> compact_thread;
		std::atomic<size_t> working_compact_thread = 0;
		std::atomic<bool> progressing_read_version = false;
		bool close = false;
		//compaction loop for background thread
		void CompactLoop();
		void ProgressReadVersion();
		void ProgressReadRelVersion();
		time_t get_read_time();
		Version* get_read_version();
		RelVersion* get_read_rel_version();


		friend class Transaction;
	};
}