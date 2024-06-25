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
#include "Version.h"
#include "BACH/db/DB.h"
#include "BACH/file/FileWriter.h"
#include "BACH/label/LabelManager.h"
#include "BACH/utils/options.h"

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

		//std::shared_ptr<EdgeEntry> FindEdge(vertex_t src, vertex_t dst, label_t label, time_t now_epoch);
		
	private:
		DB* db;
		std::mutex CompactionCVMutex;
		std::condition_variable CompactionCV;
		std::queue<Compaction> CompactionList;

		std::shared_ptr<Version> current_version;
	};
}