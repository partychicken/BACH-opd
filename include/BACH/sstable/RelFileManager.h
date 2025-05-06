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

        void AddCompaction(RelCompaction<std::string> &compaction, bool high = true);

        VersionEdit *MergeRelFile(Compaction &compaction);

        idx_t GetFileID() {
            return ++id;
        };

    private:
        DB *db;
		std::mutex HighCompactionCVMutex;
        std::condition_variable HighCompactionCV;
        std::queue<RelCompaction<std::string>> HighCompactionList;
		std::mutex LowCompactionCVMutex;
        std::condition_variable LowCompactionCV;
        std::queue<RelCompaction<std::string>> LowCompactionList;
        std::vector< //level
            std::vector< //key-min
                idx_t> > FileNumList;
        // std::vector<OrderedDictionary > **DictList;
        std::vector<std::vector<OrderedDictionary >> DictList;
        int id = 0;
        friend class DB;
    };
}
