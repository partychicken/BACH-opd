#include "BACH/memory/TupleEntry.h"

namespace BACH
{
	typedef std::string tp_key;
    void TupleIndexEntry::AddTuple(Tuple tuple, tp_key key, time_t timestamp) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        TPSizeIndex->AddTuple(tuple, key, timestamp);
    }



    void TupleIndexEntry::DeleteTuple(tp_key key, time_t timestamp) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        TPSizeIndex->DeleteTuple(key, timestamp);
    }

    Tuple TupleIndexEntry::GetTuple(tp_key key, time_t timestamp) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return TPSizeIndex->GetTuple(key, timestamp);
    }

    void TupleIndexEntry::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        TPSizeIndex->UpdateTuple(tuple, key, timestamp);
    }

    std::vector<Tuple> TupleIndexEntry::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return TPSizeIndex->ScanTuples(start_key, end_key, timestamp);
    }

    // currently not consider primary key conflict
    // 智能指针少点用
    void TPSizeEntry::AddTuple(Tuple tuple, tp_key key, time_t timestamp) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        auto tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, nullptr);
        tuple_pool.push_back(tuple_entry);
        SkipList::Accessor accessor(tuple_index);
        //accessor.insert(std::make_pair(key, (idx_t)(tuple_pool.size() - 1)));
        total_tuple.fetch_add(1);
    }
    // 插入墓碑
    void TPSizeEntry::DeleteTuple(tp_key key, time_t timestamp) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        SkipList::Accessor accessor(tuple_index);
        auto it = accessor.find({ key, 0 });
        if (it != accessor.end()) {
            auto tuple_entry = tuple_pool[it->second];
            tuple_entry->time = timestamp;
            del_table[key] = timestamp;
        }
    }


    Tuple TPSizeEntry::GetTuple(tp_key key, time_t timestamp) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        SkipList::Accessor accessor(tuple_index);
        auto it = accessor.find({ key, 0 });
        if (it != accessor.end()) {
            auto tuple_entry = tuple_pool[it->second];
            if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
                return *tuple_entry->tuple;
            }
        }
        return Tuple(); // 返回空的 Tuple 表示未找到
    }

    void TPSizeEntry::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        SkipList::Accessor accessor(tuple_index);
        auto it = accessor.find({ key, 0 });
        if (it != accessor.end()) {
            auto tuple_entry = tuple_pool[it->second];
            if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
                auto new_tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, tuple_entry);
                tuple_pool.push_back(new_tuple_entry);
                //accessor.insert(std::make_pair(key, tuple_pool.size() - 1));
                total_tuple.fetch_add(1);
            }
        }
    }

    std::vector<Tuple> TPSizeEntry::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        std::vector<Tuple> result;
        SkipList::Accessor accessor(tuple_index);
        for (auto it = accessor.lower_bound({ start_key, 0 }); it != accessor.end() && it->first <= end_key; ++it) {
            auto tuple_entry = tuple_pool[it->second];
            if (tuple_entry->time <= timestamp && del_table.find(it->first) == del_table.end()) {
                result.push_back(*tuple_entry->tuple);
            }
        }
        return result;
    }

}