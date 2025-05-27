#include "BACH/memory/RowMemoryManager.h"
#include "BACH/sstable/RelVersion.h"
#include "BACH/db/DB.h"

namespace BACH {
    rowMemoryManager::rowMemoryManager(DB *_db, idx_t _column_num) : db(_db), column_num(_column_num), memtable_cnt(1) {
        currentMemTable = std::make_shared<relMemTable>(0, column_num);
    }

    void rowMemoryManager::PutTuple(Tuple tuple, tp_key key, time_t timestamp, bool tombstone) {
        std::shared_lock<std::shared_mutex> lock(currentMemTable->mutex);
        while(!currentMemTable->PutTuple(tuple, key, timestamp, tombstone));
        CheckAndImmute();
    }


    //void rowMemoryManager::AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
    //	std::unique_lock<std::shared_mutex> lock(currentMemTable->mutex);
    //	currentMemTable->AddTuple(tuple, key, timestamp, property);
    //	current_data_count++;
    //	CheckAndPersist();
    //}


    //void rowMemoryManager::DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property) {
    //	std::unique_lock<std::shared_mutex> lock(currentMemTable->mutex);
    //	currentMemTable->DeleteTuple(key, timestamp, property);
    //	current_data_count--;
    //	CheckAndPersist();
    //}

    Tuple rowMemoryManager::GetTuple(tp_key key, time_t timestamp, RelVersion *version) {
        auto x = currentMemTable;
        while (x)  {
            //std::shared_lock<std::shared_mutex> lock(x->mutex);
            auto now_res = x->GetTuple(key, timestamp);
            if (!now_res.row.empty()) {
                return now_res;
            }
            if (x == version->size_entry) break;
            x = x->next.lock();
        }
        return Tuple();
    }

    void rowMemoryManager::GetKTuple(idx_t k, tp_key key, time_t timestamp, std::vector<std::unique_ptr<Tuple>> &am, RelVersion *version) {
        auto x = currentMemTable;
        while (x)  {
            //std::shared_lock<std::shared_mutex> lock(x->mutex);
            x->GetKTuple(k, key, timestamp, am);
            if (x == version->size_entry) break;
            x = x->next.lock();
        }
    }

    //void rowMemoryManager::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
    //	std::unique_lock<std::shared_mutex> lock(currentMemTable->mutex);
    //	currentMemTable->UpdateTuple(tuple, key, timestamp, property);
    //}

    //TODO: change code to straightly insert into answer-merger.
    std::vector<Tuple> rowMemoryManager::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
        std::shared_lock<std::shared_mutex> lock(currentMemTable->mutex);

        auto x = currentMemTable;
        while (x) {
            //std::shared_lock<std::shared_mutex> lock(x->mutex);
            auto now_res = x->ScanTuples(start_key, end_key, timestamp);
            x = x->next.lock();
        }
        return std::vector<Tuple>();
    }


    void rowMemoryManager::CheckAndImmute() {
        if (currentMemTable->current_data_count >= db->options->MEM_TABLE_MAX_SIZE) 
            if (!currentMemTable->immutable.exchange(true)) {
                immute_memtable(currentMemTable);
                memtable_cnt.fetch_add(1, std::memory_order_relaxed);
                if (memtable_cnt.load() > db->options->MAX_MEMTABLE_NUM) {
                    db->StallWrite(0);
                }
            }
        return;
    }

    // ���ڵ�ǰ����������L0Ϊtileing���û��Ŀ϶���CurrentTupleEntry,���Ҿ��Ƕ����Ǹ�
    // ����Ҳ��Ҫ����һ���µ�memtable
    void rowMemoryManager::immute_memtable(std::shared_ptr<relMemTable> memtable) {
        auto new_memtable = std::make_shared<relMemTable>(0, memtable->column_num, memtable);
        currentMemTable = new_memtable;
        memtable->sema.release(1024);
        RelCompaction<std::string> x;
        x.label_id = 0;
        x.target_level = 0;
        x.vertex_id_b = 0;
        x.relPersistence = memtable;
        x.key_min = memtable->min_key;
        db->relFiles->AddCompaction(x);
    }


    VersionEdit *rowMemoryManager::RowMemtablePersistence(idx_t file_id, std::shared_ptr<relMemTable> memtable) {
        std::string key_min = memtable->min_key, key_max = memtable->max_key;
        // key_min.resize(db->options->KEY_SIZE);
        // key_max.resize(db->options->KEY_SIZE);
        auto temp_file_metadata = new RelFileMetaData(0, 0, 0, file_id,
                                                      "", key_min, key_max, memtable->total_tuple,
                                                      memtable->column_num - 1);
        std::string file_name = temp_file_metadata->file_name;
        auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/" + file_name, false);
        RelFileBuilder<std::string> rfb(fw, db->options);
        std::unique_lock<std::shared_mutex> lock(memtable->mutex);
        TempColumn tmp(memtable.get(), memtable->total_tuple, memtable->column_num);
        idx_t **data = new idx_t *[memtable->column_num];
        std::vector<OrderedDictionary *> dicts(memtable->column_num - 1);
        for (size_t i = 0; i < memtable->column_num - 1; i++) {
            data[i] = new idx_t[memtable->total_tuple];
            // dicts[i] = new OrderedDictionary();
            // dicts[i]->importData(tmp.GetColumn(i + 1), memtable->total_tuple);
            // dicts[i]->CompressData(data[i], tmp.GetColumn(i + 1), memtable->total_tuple);
            // temp_file_metadata->dictionary.push_back(*dicts[i]);
            // delete dicts[i];
            temp_file_metadata->dictionary.emplace_back(OrderedDictionary());
            auto nsiz = temp_file_metadata->dictionary.size();
            auto &now_dict = temp_file_metadata->dictionary[nsiz - 1];
            now_dict.importData(tmp.GetColumn(i + 1), memtable->total_tuple);
            now_dict.CompressData(data[i], tmp.GetColumn(i + 1), memtable->total_tuple);
        }
        rfb.ArrangeRelFileInfo(tmp.GetColumn(0), memtable->total_tuple, db->options->KEY_SIZE, memtable->column_num - 1,
                               data);
        temp_file_metadata->bloom_filter = BloomFilter(memtable->total_tuple, db->options->FALSE_POSITIVE);
        for (size_t i = 0; i < memtable->total_tuple; i++) {
            temp_file_metadata->bloom_filter.insert(tmp.GetColumn(0)[i]);
        }
        temp_file_metadata->block_count = rfb.GetBlockCount();
        // temp_file_metadata->block_filter_size = rfb.GetBlockFilterSize();
        // temp_file_metadata->last_block_filter_size = rfb.GetLastBlockFilterSize();
        temp_file_metadata->block_meta_begin_pos = rfb.GetBlockMetaBeginPos();
        // temp_file_metadata->block_func_num = rfb.GetBlockFuncNum();
        /*delete data;*/
        for (size_t i = 0; i < memtable->column_num - 1; i++) {
            delete data[i];
        }
        delete[] data;

        memtable_cnt.fetch_add(-1, std::memory_order_relaxed);
        if (memtable_cnt.load() <= db->options->MAX_MEMTABLE_NUM) {
            db->ResumeWrite(0);
        }

        auto vedit = new VersionEdit();
        temp_file_metadata->file_size = fw->file_size();
        vedit->EditFileList.push_back(temp_file_metadata);
        return vedit;
    }


    void rowMemoryManager::FilterByValueRange(time_t timestamp, const std::function<bool(Tuple &)> &func,
                                              AnswerMerger &am, RelVersion *version) {
        auto x = currentMemTable;
        while (x) {
            //std::shared_lock<std::shared_mutex> lock(x->mutex);
            x->FilterByValueRange(timestamp, func, am);
            if (x == version->size_entry) break;
            x = x->next.lock();
        }
    }

    //template<typename func>
    std::function<bool(Tuple &)> CreateValueFilterFunction(const idx_t column_idx, const std::string &value_min,
                                                           const std::string &value_max) {
        return [value_min, value_max, column_idx](Tuple &tuple) {
            if (tuple.row[column_idx] >= value_min && tuple.row[column_idx] <= value_max) {
                return true;
            }
            return false;
        };
    }
}
