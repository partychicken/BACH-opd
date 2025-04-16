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

namespace BACH
{

	typedef std::string tp_key;

	class DB;

    // 当前是个非拜占庭的模型，输入的tuple并没有硬性规定列数
    // 暂时没有限制这个事,默认输入数据合法
	class rowMemoryManager
	{
    public:
        rowMemoryManager() = delete;
        rowMemoryManager(const rowMemoryManager&) = delete;
        rowMemoryManager& operator=(const rowMemoryManager&) = delete;
        rowMemoryManager(DB* _db, idx_t _column_num);
        ~rowMemoryManager() = default;

        void PutTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);

        //void AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
        //void DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property);
        Tuple GetTuple(tp_key key, time_t timestamp);
        //void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
        std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);

        void immute_memtable(std::shared_ptr<relMemTable> size_info);
        VersionEdit* RowMemtablePersistence(idx_t file_id, std::shared_ptr < relMemTable > size_info);
		
        std::vector<Tuple> FilterByValueRange(time_t timestamp, const std::function<bool(Tuple&)>& func);

    private:
        ConcurrentArray<std::shared_ptr<relMemTable>> memTable;
        std::shared_ptr<relMemTable> currentMemTable;
        DB* db;
        size_t data_count_threshold = 1024; // 数据量阈值
        size_t current_data_count = 0; // 当前数据量计数
		size_t column_num = 0; // 列数
        void CheckAndImmute();
	};

    // value filter function factory
	std::function<bool(Tuple&)> CreateValueFilterFunction(const idx_t column_idx, const std::string& value_min,
        const std::string& value_max);


}