#include "BACH/memory/RowMemoryManager.h"
#include "BACH/db/DB.h"

namespace BACH
{
	rowMemoryManager::rowMemoryManager(DB* _db) :db(_db){ 
		memTable.push_back(std::make_shared<relMemTable>(0));
		currentMemTable = memTable[0];
	}

	void rowMemoryManager::PutTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
		std::unique_lock<std::shared_mutex> lock(currentMemTable->mutex);
		currentMemTable-> PutTuple(tuple, key, timestamp, property);
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
		std::shared_lock<std::shared_mutex> lock(currentMemTable->mutex);
		return currentMemTable->GetTuple(key, timestamp);
	}

	//void rowMemoryManager::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property) {
	//	std::unique_lock<std::shared_mutex> lock(currentMemTable->mutex);
	//	currentMemTable->UpdateTuple(tuple, key, timestamp, property);
	//}

	std::vector<Tuple> rowMemoryManager::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
		std::shared_lock<std::shared_mutex> lock(currentMemTable->mutex);
		return currentMemTable->ScanTuples(start_key, end_key, timestamp);
	}


	// TODO: compact and go column
	void rowMemoryManager::CheckAndImmute(tp_key key_min) {
		if (current_data_count >= db->options->MEM_TABLE_MAX_SIZE) {
			immute_memtable(currentMemTable, key_min);
			current_data_count = 0;

		}return;
	}

	// 由于当前数据特征，L0为tileing，置换的肯定是CurrentTupleEntry,并且就是顶上那个
	// 所以也需要插入一个新的memtable
	void rowMemoryManager::immute_memtable(std::shared_ptr<relMemTable> memtable) {
		auto new_memtable = std::make_shared<relMemTable>(0, memtable);
		memtable->last = new_memtable;
		memTable[memTable.size()] = new_memtable;
		memtable->sema.release(1024);
		RelCompaction<std::string> x;
		x.label_id = 0;
		x.target_level = 0;
		x.vertex_id_b = 0;
		x.relPersistence = memtable;
		x.key_min = memtable->min_key;
		db->Files->AddCompaction(x);

	}


	void rowMemoryManager::RowMemtablePersistence(idx_t file_id, std::shared_ptr < relMemTable > size_info) {


	}

}