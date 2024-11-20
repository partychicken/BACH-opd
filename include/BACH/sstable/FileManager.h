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
#include "BACH/file/FileWriter.h"
#include "BACH/label/LabelManager.h"
#include "BACH/utils/Options.h"
#include "BACH/sstable/SSTableParser.h"
#include "BACH/sstable/SSTableBuilder.h"
#include "BACH/sstable/Version.h"

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
		VersionEdit* MergeSSTable(Compaction& compaction);
		idx_t GetFileID(
			label_t label, idx_t level, vertex_t src_b);

	private:
		DB* db;
		std::mutex CompactionCVMutex;
		std::condition_variable CompactionCV;
		std::queue<Compaction> CompactionList;
		std::vector<      //label
			std::vector<  //level
			std::vector<  //src_b
			idx_t>>> FileNumList;
		friend class DB;
	};
}