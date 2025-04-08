#include "BACH/memory/TupleEntry.h"

namespace BACH
{
	typedef std::string tp_key;
    //void TupleIndexEntry::AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
    //    std::unique_lock<std::shared_mutex> lock(mutex);
    //    TPSizeIndex->AddTuple(tuple, key, timestamp, property);
    //}



    //void TupleIndexEntry::DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property) {
    //    std::unique_lock<std::shared_mutex> lock(mutex);
    //    TPSizeIndex->DeleteTuple(key, timestamp, property);
    //}

    //Tuple TupleIndexEntry::GetTuple(tp_key key, time_t timestamp) {
    //    std::shared_lock<std::shared_mutex> lock(mutex);
    //    return TPSizeIndex->GetTuple(key, timestamp);
    //}

    //void TupleIndexEntry::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
    //    std::unique_lock<std::shared_mutex> lock(mutex);
    //    TPSizeIndex->UpdateTuple(tuple, key, timestamp, property);
    //}

    //std::vector<Tuple> TupleIndexEntry::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
    //    std::shared_lock<std::shared_mutex> lock(mutex);
    //    return TPSizeIndex->ScanTuples(start_key, end_key, timestamp);
    //}

    // currently not consider primary key conflict
    // ����ָ���ٵ���
    // ��updateͬ
    //void relMemTable::AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {

    //    auto tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, property);
    //    tuple_pool.push_back(tuple_entry);
    //    RelSkipList::Accessor accessor(tuple_index);
    //    accessor.insert(std::make_pair(key, (idx_t)(tuple_pool.size() - 1)));
    //    total_tuple.fetch_add(1);
    //}
    // ����Ĺ��
    //void relMemTable::DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property) {

    //    RelSkipList::Accessor accessor(tuple_index);
    //    auto it = accessor.find({ key, 0 });
    //    if (it != accessor.end()) {
    //        auto tuple_entry = tuple_pool[it->second];
    //        if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
    //            auto new_tuple_entry = std::make_shared<TupleEntry>(nullptr, timestamp, property, it->second);
    //            size_t pos = tuple_pool.push_back(new_tuple_entry);
    //            /*accessor.insert(std::make_pair(key, tuple_pool.size() - 1));*/
    //            it->second = pos;
    //            del_table[key] = timestamp;
    //            total_tuple.fetch_add(1);
    //        }
    //    }

    //}


    Tuple relMemTable::GetTuple(tp_key key, time_t timestamp) {

        RelSkipList::Accessor accessor(tuple_index);
        auto it = accessor.find({ key, 0 });
        if (it != accessor.end()) {
            auto tuple_entry = tuple_pool[it->second];
            if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
                return *tuple_entry->tuple;
            }
        }
        return Tuple(); // ���ؿյ� Tuple ��ʾδ�ҵ�
    }


    // put ����add������
    //void relMemTable::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {

    //    RelSkipList::Accessor accessor(tuple_index);
    //    auto it = accessor.find({ key, 0 });
    //    if (it != accessor.end()) {
    //        auto tuple_entry = tuple_pool[it->second];
    //        if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
    //            auto new_tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, property, it->second);
    //            size_t pos = tuple_pool.push_back(new_tuple_entry);
    //            /*accessor.insert(std::make_pair(key, tuple_pool.size() - 1));*/
				//it->second = pos;
    //            total_tuple.fetch_add(1);
    //        }
    //    }
    //}

    void relMemTable::PutTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {

        RelSkipList::Accessor accessor(tuple_index);
        auto it = accessor.find({ key, 0 });
        if (it != accessor.end()) {
            auto new_tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, property, it->second);
            size_t pos = tuple_pool.push_back(new_tuple_entry);
            /*accessor.insert(std::make_pair(key, tuple_pool.size() - 1));*/
            it->second = pos;
            total_tuple.fetch_add(1);
		}
        else {
            auto tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, property);
            tuple_pool.push_back(tuple_entry);
            accessor.insert(std::make_pair(key, (idx_t)(tuple_pool.size() - 1)));
            total_tuple.fetch_add(1);
        }
    }


    std::vector<Tuple> relMemTable::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {

        std::vector<Tuple> result;
        RelSkipList::Accessor accessor(tuple_index);
        for (auto it = accessor.lower_bound({ start_key, 0 }); it != accessor.end() && it->first <= end_key; ++it) {
            auto tuple_entry = tuple_pool[it->second];
            if (tuple_entry->time <= timestamp && del_table.find(it->first) == del_table.end()) {
                result.push_back(*tuple_entry->tuple);
            }
        }
        return result;
    }

}