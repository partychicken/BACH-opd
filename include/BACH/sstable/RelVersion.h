#pragma once

#include <algorithm>
#include <list>
#include <vector>
#include "BACH/sstable/Compaction.h"
#include "BACH/memory/TupleEntry.h"
#include "Version.h"

namespace BACH
{
    class DB;
    class RelVersion
    {
    public:
        RelVersion(DB* _db);
        RelVersion(RelVersion* _prev, VersionEdit* edit, time_t time);
        ~RelVersion();

        RelCompaction<std::string>* GetCompaction(idx_t level);
        bool AddRef();
        void DecRef();
        void AddSizeEntry(std::shared_ptr < relMemTable > x);

        std::vector<      //level
            std::vector<
            FileMetaData*>> FileIndex;
        RelVersion* next;
        time_t epoch;
        time_t next_epoch;

    private:
        std::vector<size_t> FileTotalSize;
        std::atomic<idx_t> ref = 1;
        std::shared_ptr < relMemTable > size_entry = NULL;
        DB* db;

        friend class RelVersionIterator;
        friend class rowMemoryManager;
    };

    class RelVersionIterator
    {
    public:
        RelVersionIterator(RelVersion* _version, std::string _key_min, std::string _key_max);
        ~RelVersionIterator() = default;
        FileMetaData* GetFile() const;
        bool End() const { return end; }
        void next();
    private:
        RelVersion* version;
        std::string key_min, key_max;
        idx_t level = -1;
        idx_t idx = 0;
        size_t file_size, size;
        bool end = false;
        void nextlevel();
        bool findsrc();
    };

    bool RelFileCompareWithPair(FileMetaData* lhs, const std::pair<std::string, idx_t> &rhs);

    bool RelFileCompareWithPairUpper(const std::pair<std::string, idx_t>& rhs, FileMetaData* lhs);

	bool RelFileCompare(FileMetaData* lhs, FileMetaData* rhs);
}
