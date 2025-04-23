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
        //auto it = accessor.find({ key, 0 });
        //if (it != accessor.end()) {
        //    auto tuple_entry = tuple_pool[it->second];
        //    if (tuple_entry->time <= timestamp && del_table.find(key) == del_table.end()) {
        //        return *tuple_entry->tuple;
        //    }
        //}

        // lower_bound����ֵ��Ϊ����ֵ�����Ǻ�һ����¼
        auto it = accessor.lower_bound(std::make_tuple(key, timestamp, TupleEntry()));
        if (it != accessor.end()) {
			//while (std::get<0>(*it) < key || (std::get<0>(*it) == key && std::get<1>(*it) > timestamp)) {
			//	++it;
			//}
            if (std::get<0>(*it) == key && std::get<1>(*it) < timestamp) {
                const auto& key = *it;

                BACH::TupleEntry entry = std::get<2>(key);

				return entry.tuple;
            }
            else {
                std::cout << "Key not found" << std::endl;
            }
		}
		else {
			std::cout << "Key not found" << std::endl;
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
        auto it = accessor.lower_bound(std::make_tuple(key, timestamp, TupleEntry()));
		//while (std::get<0>(*it) < key || (std::get<0>(*it) == key && std::get<1>(*it) > timestamp)) {
		//	++it;
		//}
        if (it != accessor.end() && std::get<0>(*it) == key ) {
            auto new_tuple_entry =  new TupleEntry(tuple, timestamp, property, &std::get<2>(*it));
            accessor.insert(std::make_tuple(key, timestamp, *new_tuple_entry));
            total_tuple.fetch_add(1);
		}
        else {
            auto new_tuple_entry = new TupleEntry(tuple, timestamp, property);
            accessor.insert(std::make_tuple(key, timestamp, *new_tuple_entry));
            total_tuple.fetch_add(1);
        }
        UpdateMinMax(key);
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
                result.push_back(tuple_entry.tuple);
				last_key = std::get<0>(*it);
            }
        }
        return result;
    }

    void relMemTable::UpdateMinMax(tp_key key) {
        if (key > max_key) {
            max_key = key;
        }
        if (key < min_key) {
            min_key = key;
        }
    }

    void relMemTable::FilterByValueRange(time_t timestamp, const std::function<bool(Tuple&)>& func, AnswerMerger& am) {
		/*std::vector<Tuple> result;*/
		RelSkipList::Accessor accessor(tuple_index);
		tp_key last_key = "";
		for (auto it = ++accessor.begin(); it != accessor.end(); ++it) {
			if (std::get<1>(*it) > timestamp || last_key == std::get<0>(*it))
				continue;
			auto tuple_entry = std::get<2>(*it);
			if (tuple_entry.property != TOMBSTONE && tuple_entry.property != NONEINDEX) {
				if (func(tuple_entry.tuple)) {
					/*result.push_back(*tuple_entry.tuple);*/
					am.insert_answer(std::get<0>(*it), std::move(tuple_entry.tuple), false);
                    last_key = std::get<0>(*it);
				}
			}
		}
		
    }

}