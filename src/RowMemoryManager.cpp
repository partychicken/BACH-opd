#include "BACH/memory/RowMemoryManager.h"
#include "BACH/db/DB.h"

namespace BACH {
    rowMemoryManager::rowMemoryManager(DB *_db, idx_t _column_num) : db(_db), column_num(_column_num) {
        memTable.push_back(std::make_shared<relMemTable>(0, column_num));
        currentMemTable = memTable[0];
    }

    void rowMemoryManager::PutTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
        std::unique_lock<std::shared_mutex> lock(currentMemTable->mutex);
        currentMemTable->PutTuple(tuple, key, timestamp, property);
        current_data_count++;
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

    Tuple rowMemoryManager::GetTuple(tp_key key, time_t timestamp) {
        auto x = currentMemTable;
        while (x) {
            std::shared_lock<std::shared_mutex> lock(x->mutex);
            auto now_res = x->GetTuple(key, timestamp);
            if (!now_res.row.empty()) {
                return now_res;
            }
            x = x->next;
        }
        return Tuple();
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
            std::shared_lock<std::shared_mutex> lock(x->mutex);
            auto now_res = x->ScanTuples(start_key, end_key, timestamp);
            x = x->next;
        }
        return std::vector<Tuple>();
    }


    void rowMemoryManager::CheckAndImmute() {
        if (current_data_count >= db->options->MEM_TABLE_MAX_SIZE) {
            immute_memtable(currentMemTable);
            current_data_count = 0;
            // memTable.push_back(std::make_shared<relMemTable>(0, column_num));
            currentMemTable = memTable[memTable.size() - 1];
        }
        return;
    }

    // ���ڵ�ǰ����������L0Ϊtileing���û��Ŀ϶���CurrentTupleEntry,���Ҿ��Ƕ����Ǹ�
    // ����Ҳ��Ҫ����һ���µ�memtable
    void rowMemoryManager::immute_memtable(std::shared_ptr<relMemTable> memtable) {
        auto new_memtable = std::make_shared<relMemTable>(0, memtable->column_num, memtable);
        memtable->last = new_memtable;
        memTable.push_back(new_memtable);
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
        auto temp_file_metadata = new RelFileMetaData(0, 0, 0, file_id,
                                                      "", memtable->min_key, memtable->max_key, memtable->total_tuple,
                                                      memtable->column_num);
        std::string file_name = temp_file_metadata->file_name;
        auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/" + file_name, false);
        RelFileBuilder<std::string> rfb(fw, db->options);
        // �Ƚ�����ת�У��ٽ����ֵ�ѹ����֮�����rfb��ArrangeRelFileInfo
        TempColumn tmp(memtable.get(), memtable->total_tuple, memtable->column_num);
        idx_t **data = new idx_t *[memtable->column_num];
        std::vector<OrderedDictionary *> dicts(memtable->column_num - 1);
        for (size_t i = 0; i < memtable->column_num - 1; i++) {
            data[i] = new idx_t[memtable->total_tuple];
            dicts[i] = new OrderedDictionary();
            dicts[i]->importData(tmp.GetColumn(i + 1), memtable->total_tuple);
            dicts[i]->CompressData(data[i], tmp.GetColumn(i + 1), memtable->total_tuple);
            temp_file_metadata->dictionary.push_back(*dicts[i]);
            delete dicts[i];
        }
        std::cout << "Arrived 2" << std::endl;
        rfb.ArrangeRelFileInfo(tmp.GetColumn(0), memtable->total_tuple, 64, memtable->column_num - 1, data);

        std::cout << "Arrived" << std::endl;
        /*delete data;*/
        for (size_t i = 0; i < memtable->column_num; i++) {
            delete data[i];
        }
        delete[] data;

        auto vedit = new VersionEdit();
        temp_file_metadata->file_size = fw->file_size();
        vedit->EditFileList.push_back(temp_file_metadata);
        return vedit;
    }


    void rowMemoryManager::FilterByValueRange(time_t timestamp, const std::function<bool(Tuple &)> &func,
                                              AnswerMerger &am) {
        auto x = currentMemTable;
        while (x) {
            std::shared_lock<std::shared_mutex> lock(x->mutex);
            x->FilterByValueRange(timestamp, func, am);
        }
    }

    template<typename func>
    std::function<bool(Tuple &)> CreateValueFilterFunction(const idx_t column_idx, const func &value_min,
                                                           const func &value_max) {
        return [value_min, value_max, column_idx](Tuple &tuple) {
            if (tuple.GetRow(column_idx) >= value_min && tuple.GetRow(column_idx) <= value_max) {
                return true;
            }
            return false;
        };
    }
}
