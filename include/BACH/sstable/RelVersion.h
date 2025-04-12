#pragma once

#include <algorithm>
#include <list>
#include <vector>
#include "BACH/sstable/Compaction.h"
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

        template<typename Key_t>
        RelCompaction<Key_t>* GetCompaction(VersionEdit* edit, bool force_level = false);
        bool AddRef();
        void DecRef();
        void AddSizeEntry(std::shared_ptr < SizeEntry > x);

        std::vector<      //level
            std::vector<
            FileMetaData*>> FileIndex;
        RelVersion* next;
        time_t epoch;
        time_t next_epoch;

    private:
        std::vector<size_t> FileTotalSize;
        std::atomic<idx_t> ref = 1;
        std::shared_ptr < SizeEntry > size_entry = NULL;
        DB* db;

        template<typename Key_t>
        friend class RelVersionIterator;
    };

    template<typename Key_t>
    class RelVersionIterator
    {
    public:
        RelVersionIterator(RelVersion* _version, Key_t _key_min);
        ~RelVersionIterator() = default;
        FileMetaData* GetFile() const;
        bool End() const { return end; }
        void next();
    private:
        RelVersion* version;
        Key_t key_now;
        idx_t level = -1;
        idx_t idx = 0;
        size_t file_size, size;
        bool end = false;
        void nextlevel();
        bool findsrc();
    };

    template<typename Key_t>
    bool RelFileCompareWithPair(FileMetaData* lhs, std::pair<vertex_t, idx_t> rhs);

    template<typename Key_t>
    bool RelFileCompare(FileMetaData* lhs, FileMetaData* rhs);
}
