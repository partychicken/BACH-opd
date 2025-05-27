#include "BACH/memory/TupleEntry.h"

namespace BACH {
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
        auto it = accessor.find(std::make_pair(key, 0));
        if (it != accessor.end()) {
            //if (it->tuple.GetKey() == key && it->time <= timestamp) {
            //    BACH::TupleEntry entry = *it;
            //    if (it->property == TOMBSTONE) return Tuple();
            //    return entry.tuple;
            //}
            if (it->first != key) {
                return Tuple();
            }
            auto found = it->second;
            while(found != NONEINDEX) {
                if (tuple_pool[found]->time <= timestamp)
                {
                    if (tuple_pool[found]->tombstone)
                        return Tuple();
                    else
                        return tuple_pool[found]->tuple;
                }
                found = tuple_pool[found]->next;
            }
        }
        return Tuple(); // ���ؿյ� Tuple ��ʾδ�ҵ�
    }

    bool relMemTable::PutTuple(Tuple tuple, tp_key key, time_t timestamp, bool tombstone) {
        
        if(immutable) {
            sema.try_acquire();
			return false;
        }

        RelSkipList::Accessor accessor(tuple_index);
        
        //accessor.insert(TupleEntry(tuple, timestamp, property));
        
        idx_t found;
		auto it = accessor.find(std::make_pair(key, 0));
        if (it != accessor.end()) {
			found = it->second;
        }
        else
			found = NONEINDEX;
        auto pos = tuple_pool.push_back(new TupleEntry(tuple, timestamp, tombstone, found));
		if (found == NONEINDEX) {
			accessor.insert(std::make_pair(key, pos));
            total_tuple.fetch_add(1);
		}
		else {
			it->second = pos;
		}
		this->max_time = std::max(timestamp, this->max_time);
        size += Options::KEY_SIZE;
        UpdateMinMax(key);
        ++current_data_count;
        return true;
    }


    std::vector<Tuple> relMemTable::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
        std::vector<Tuple> result;
        RelSkipList::Accessor accessor(tuple_index);
        tp_key last_key = "";
        auto it = accessor.lower_bound(std::make_pair(start_key, 0));
        for (; it != accessor.end() && tuple_pool[it->second]->tuple.row[0] <= end_key; ++it) {
            if (!tuple_pool[it->second]->tombstone) {
				result.push_back(tuple_pool[it->second]->tuple);
            }        
        }
        return result;
    }

    void relMemTable::GetKTuple(idx_t k, tp_key start_key, time_t timestamp, std::vector<std::unique_ptr<Tuple>> &am) {
        RelSkipList::Accessor accessor(tuple_index);
        idx_t kk = k;
        auto it = accessor.lower_bound(std::make_pair(start_key, 0));
        std::vector<std::unique_ptr<Tuple>> nowa;
        std::vector<std::unique_ptr<Tuple>> as;
        for (; it != accessor.end(); ++it) {
            auto found = it->second;
            while(found != NONEINDEX) {
                if (tuple_pool[found]->time <= timestamp) {
                    if (!tuple_pool[found]->tombstone) {
                        //Tuple tmp (tuple_pool[found]->tuple);
                        nowa.emplace_back(std::make_unique<Tuple>(tuple_pool[found]->tuple));
                        k--;
                        if (!k) goto end;
                    }
                    break;
                }
                found = tuple_pool[found]->next;
            }
        }
        end:
        size_t i = 0, j = 0;
        while (i < nowa.size() && j < am.size()) {
            auto x = nowa[i]->row[0] <=> am[j]->row[0];
            if (x < 0) {
                as.emplace_back(std::move(nowa[i]));
                i++;
            } else if (x > 0) {
                as.emplace_back(std::move(am[j]));
                j++;
            } else {
                // if equal, we prefer the one in nowa
                as.emplace_back(std::move(am[j]));
                i++;
                j++;
            }
        }
        while (i < nowa.size()) {
            as.emplace_back(std::move(nowa[i]));
            i++;
        }
        while (j < am.size()) {
            as.emplace_back(std::move(am[j]));
            j++;
        }
        if(as.size() > kk) {
            as.resize(kk);
        }
        am = std::move(as);
    }


    void relMemTable::UpdateMinMax(tp_key key) {
        if (key > max_key) {
            max_key = key;
        }
        if (key < min_key) {
            min_key = key;
        }
        if (min_key == "") {
            min_key = key;
        }
    }

    void relMemTable::FilterByValueRange(time_t timestamp, const std::function<bool(Tuple &)> &func, AnswerMerger &am) {
        /*std::vector<Tuple> result;*/
        RelSkipList::Accessor accessor(tuple_index);
        for (auto it = accessor.begin(); it != accessor.end(); ++it) {
            if (am.has_answer(it->first)) {
                continue;
            }
            auto found = it->second;
            while(found != NONEINDEX) {
                if (tuple_pool[found]->time <= timestamp) {
                    if (!tuple_pool[found]->tombstone) {
                        if (func(tuple_pool[found]->tuple)) {
                            Tuple tmp (tuple_pool[found]->tuple);
                            am.insert_answer(tuple_pool[found]->tuple.row[0], std::move(tmp), false);
                        }
                    }
                    break;
                }
                found = tuple_pool[found]->next;
            }
        }
    }
}
