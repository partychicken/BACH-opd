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

namespace BACH
{

	typedef std::string tp_key;

	class DB;
	class FormatRow
	{
    public:
        FormatRow() = delete;
        FormatRow(const FormatRow&) = delete;
        FormatRow& operator=(const FormatRow&) = delete;
        FormatRow(DB* _db);
        ~FormatRow() = default;

        void AddTuple(Tuple tuple, tp_key key, time_t timestamp);
        void DeleteTuple(tp_key key, time_t timestamp);
        Tuple GetTuple(tp_key key, time_t timestamp);
        void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp);
        std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);

    private:
        ConcurrentArray<std::shared_ptr<TupleIndexEntry>> TupleIndex;
        std::shared_ptr<TupleIndexEntry> CurrentTupleIndexEntry;
        DB* db;
        size_t data_count_threshold = 1000; // 数据量阈值
        size_t current_data_count = 0; // 当前数据量计数

        void CheckAndPersist();
	};
}