#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <sul/dynamic_bitset.hpp>
#include "VertexLabelIdEntry.h"
#include "VertexEntry.h"
#include "BACH/label/LabelManager.h"
#include "BACH/property/PropertyFileBuilder.h"
#include "BACH/property/PropertyFileParser.h"
#include "BACH/sstable/SSTableBuilder.h"
#include "BACH/sstable/Version.h"
#include "BACH/utils/types.h"
#include "BACH/utils/Options.h"
#include "BACH/memory/TupleEntry.h"
#include "BACH/common/tuple.h"
#include "BACH/sstable/Compaction.h"
#include "BACH/sstable/RelFileBuilder.h"
#include "BACH/memory/RowToColumn.h"
#include "BACH/compress/ordered_dictionary.h"
#include "BACH/memory/AnswerMerger.h"

namespace BACH {
    typedef std::string tp_key;

    class DB;
    class RelVersion;

    class rowMemoryManager {
    public:
        rowMemoryManager() = delete;

        rowMemoryManager(const rowMemoryManager &) = delete;

        rowMemoryManager &operator=(const rowMemoryManager &) = delete;

        rowMemoryManager(DB *_db, idx_t _column_num);

        ~rowMemoryManager() = default;

        void PutTuple(Tuple tuple, tp_key key, time_t timestamp, bool tombstone = false);

        //void AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
        //void DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property);
        Tuple GetTuple(tp_key key, time_t timestamp, RelVersion *version);

        //void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
        std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);

        void immute_memtable(std::shared_ptr<relMemTable> size_info);

        VersionEdit *RowMemtablePersistence(idx_t file_id, std::shared_ptr<relMemTable> size_info);

        void FilterByValueRange(time_t timestamp, const std::function<bool(Tuple &)> &func, AnswerMerger &am, RelVersion *version);

        void GetKTuple(idx_t k, std::string key, time_t timestamp, std::map<std::string, Tuple> &am, RelVersion *version);

    private:
        std::shared_ptr<relMemTable> currentMemTable;
        DB *db;
        size_t data_count_threshold = 1024; // ��������ֵ
        size_t current_data_count = 0; // ��ǰ����������
        size_t column_num = 0; // ����
        std::atomic<size_t> memtable_cnt;
        void CheckAndImmute();
    };

    // value filter function factory
    std::function<bool(Tuple &)> CreateValueFilterFunction(const idx_t column_idx, const std::string &value_min,
                                                           const std::string &value_max);
}
