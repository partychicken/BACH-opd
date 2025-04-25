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
#include "BACH/common/tuple.h"
#include "BACH/compress/ordered_dictionary.h"

namespace BACH {
    class DB;

    template<typename Key_t>
    class RelFileManager {
    public:
        RelFileManager() = delete;

        RelFileManager(const RelFileManager &) = delete;

        RelFileManager &operator=(const RelFileManager &) = delete;

        RelFileManager(DB *_db);

        ~RelFileManager() = default;

        void AddCompaction(Compaction &compaction);

        VersionEdit *MergeRelFile(Compaction &compaction);

        idx_t GetFileID() {
            return ++id;
        };

    private:
        DB *db;
        std::mutex CompactionCVMutex;
        std::condition_variable CompactionCV;
        std::queue<RelCompaction<Key_t>> CompactionList;
        std::vector< //level
            std::vector< //key-min
                idx_t> > FileNumList;
        std::vector<OrderedDictionary *> DictList;
        int id = 0;
        friend class DB;
    };
}
