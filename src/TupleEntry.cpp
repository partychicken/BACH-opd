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
    // 智能指针少点用
    // 跟update同
    //void relMemTable::AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {

    //    auto tuple_entry = std::make_shared<TupleEntry>(std::make_shared<Tuple>(tuple), timestamp, property);
    //    tuple_pool.push_back(tuple_entry);
    //    RelSkipList::Accessor accessor(tuple_index);
    //    accessor.insert(std::make_pair(key, (idx_t)(tuple_pool.size() - 1)));
    //    total_tuple.fetch_add(1);
    //}
    // 插入墓碑
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
        //auto it = accessor.find({ key, 0 });
        //if (it != accessor.end()) {
        //    auto tuple_entry = tuple_pool[it->second];
        //    if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
        //        return *tuple_entry->tuple;
        //    }
        //}

        // lower_bound返回值即为需求值或者是后一条记录
        auto it = accessor.lower_bound(std::make_tuple(key, timestamp, TupleEntry()));
        if (it != accessor.begin() || it != accessor.end()) {
			//while (std::get<0>(*it) < key || (std::get<0>(*it) == key && std::get<1>(*it) > timestamp)) {
			//	++it;
			//}
            if (std::get<0>(*it) == key && std::get<1>(*it) < timestamp) {
                const auto& key = *it;

                BACH::TupleEntry entry = std::get<2>(key);

				return *(entry.tuple);
            }
            else {
                std::cout << "Key not found" << std::endl;
            }
		}
		else {
			std::cout << "Key not found" << std::endl;
		}


        return Tuple(); // 返回空的 Tuple 表示未找到
    }


    // put 加上add的语义
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
        auto it = accessor.lower_bound(std::make_tuple(key, timestamp, TupleEntry()));
		//while (std::get<0>(*it) < key || (std::get<0>(*it) == key && std::get<1>(*it) > timestamp)) {
		//	++it;
		//}
        if ((it != accessor.end() || it != accessor.begin()) && std::get<0>(*it) == key ) {
            auto new_tuple_entry =  new TupleEntry(std::make_shared<Tuple>(tuple), timestamp, property, &std::get<2>(*it));
            accessor.insert(std::make_tuple(key, timestamp, *new_tuple_entry));
            total_tuple.fetch_add(1);
		}
        else {
            auto new_tuple_entry = new TupleEntry(std::make_shared<Tuple>(tuple), timestamp, property);
            accessor.insert(std::make_tuple(key, timestamp, *new_tuple_entry));
            total_tuple.fetch_add(1);
        }
    }


    std::vector<Tuple> relMemTable::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {

        std::vector<Tuple> result;
        RelSkipList::Accessor accessor(tuple_index);
        tp_key last_key = "";
        for (auto it = ++accessor.lower_bound(std::make_tuple(start_key, timestamp, TupleEntry())); it != accessor.end() && std::get<0>(*it) <= end_key; ++it) {
            
            if (std::get<0>(*it) == last_key || std::get<1>(*it) > timestamp)
                continue;
			
            
            auto tuple_entry = std::get<2>(*it);
            if (std::get<1>(*it) <= timestamp && (tuple_entry.property != TOMBSTONE || tuple_entry.property != NONEINDEX)) {
                result.push_back(*tuple_entry.tuple);
				last_key = std::get<0>(*it);
            }
        }
        return result;
    }

}