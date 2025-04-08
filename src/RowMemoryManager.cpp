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
		CheckAndPersist();
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
	void rowMemoryManager::CheckAndPersist() {

	}

}