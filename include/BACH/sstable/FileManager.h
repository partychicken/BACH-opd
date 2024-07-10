#pragma once
#include <algorithm>
#include <condition_variable>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <queue>
#include "BloomFilter.h"
#include "Compaction.h"
#include "BACH/db/DB.h"
#include "BACH/file/FileWriter.h"
#include "BACH/label/LabelManager.h"
#include "BACH/utils/options.h"
#include "BACH/sstable/SSTableParser.h"
#include "BACH/sstable/SSTableBuilder.h"

namespace BACH
{
	class DB;
	class FileManager
	{
	public:
		FileManager() = delete;
		FileManager(const FileManager&) = delete;
		FileManager& operator=(const FileManager&) = delete;
		FileManager(DB* _db);
		~FileManager() = default;

		void AddCompaction(Compaction& compaction);
		void MergeSSTable(Compaction& compaction);
		idx_t GetFileID(label_t label, idx_t level, vertex_t src_b);

	private:
		DB* db;
		std::mutex CompactionCVMutex;
		std::condition_variable CompactionCV;
		std::queue<Compaction> CompactionList;
		std::vector<      //label
			std::vector<  //level
			std::vector<  //src_b
			idx_t>>> FileNumList;
		//std::shared_ptr<Version> current_version;
		friend class DB;
	};
}