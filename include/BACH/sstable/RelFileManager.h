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

    class RelFileManager {
    public:
        RelFileManager() = default;

        RelFileManager(const RelFileManager &) = default;

        RelFileManager &operator=(const RelFileManager &) = default;

        RelFileManager(DB *_db);

        ~RelFileManager() = default;

        void AddCompaction(RelCompaction<std::string> &compaction);

        VersionEdit *MergeRelFile(Compaction &compaction);

        bool ListEmpty() {
            if (CompactionCVMutex.try_lock()) {
                bool empty = CompactionList.empty();
                CompactionCVMutex.unlock();
                return empty;
            }
            else {
                return false;
            }
        }

        idx_t GetFileID() {
            return ++id;
        };

    private:
        DB *db;
        std::mutex CompactionCVMutex;
        std::condition_variable CompactionCV;
        std::queue<RelCompaction<std::string>> CompactionList;
        std::vector< //level
            std::vector< //key-min
                idx_t> > FileNumList;
        std::vector<OrderedDictionary > *DictList;
        int id = 0;
        friend class DB;
    };
}
