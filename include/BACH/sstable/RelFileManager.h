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
#include "BACH/utils/Options.h"
#include "BACH/sstable/RelFileParser.h"
#include "BACH/sstable/RelFileBuilder.h"
#include "BACH/sstable/Version.h"

namespace BACH
{
    class DB;

    template<typename Key_t>
    class RelFileManager
    {
    public:
        RelFileManager() = delete;
        RelFileManager(const RelFileManager&) = delete;
        RelFileManager& operator=(const RelFileManager&) = delete;
        RelFileManager(DB* _db);
        ~RelFileManager() = default;

        void AddCompaction(Compaction& compaction);
        VersionEdit* MergeRelFile(Compaction& compaction);
        idx_t GetFileID(
            label_t label, idx_t level, vertex_t src_b);

    private:
        DB* db;
        std::mutex CompactionCVMutex;
        std::condition_variable CompactionCV;
        std::queue<Compaction> CompactionList;
        std::vector<  //level
        std::vector<  //key-min
        idx_t>> FileNumList;
        friend class DB;
    };
}