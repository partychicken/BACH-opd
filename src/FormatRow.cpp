#include "BACH/memory/FormatRow.h"
#include "BACH/db/DB.h"

namespace BACH
{
	FormatRow::FormatRow(DB* _db) :db(_db){ 
		TupleIndex.push_back(std::make_shared<TupleIndexEntry>(db->options));
		CurrentTupleIndexEntry = TupleIndex[0];
	}

	void FormatRow::AddTuple(Tuple tuple, tp_key key, time_t timestamp) {
		std::unique_lock<std::shared_mutex> lock(CurrentTupleIndexEntry->mutex);
		CurrentTupleIndexEntry->AddTuple(tuple, key, timestamp);
		current_data_count++;
		CheckAndPersist();
	}


	
	void FormatRow::DeleteTuple(tp_key key, time_t timestamp) {
		std::unique_lock<std::shared_mutex> lock(CurrentTupleIndexEntry->mutex);
		CurrentTupleIndexEntry->DeleteTuple(key, timestamp);
		current_data_count--;
		CheckAndPersist();
	}

	Tuple FormatRow::GetTuple(tp_key key, time_t timestamp) {
		std::shared_lock<std::shared_mutex> lock(CurrentTupleIndexEntry->mutex);
		return CurrentTupleIndexEntry->GetTuple(key, timestamp);
	}

	void FormatRow::UpdateTuple(Tuple tuple, tp_key key, time_t timestamp) {
		std::unique_lock<std::shared_mutex> lock(CurrentTupleIndexEntry->mutex);
		CurrentTupleIndexEntry->UpdateTuple(tuple, key, timestamp);
	}

	std::vector<Tuple> FormatRow::ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp) {
		std::shared_lock<std::shared_mutex> lock(CurrentTupleIndexEntry->mutex);
		return CurrentTupleIndexEntry->ScanTuples(start_key, end_key, timestamp);
	}


	// TODO: compact and go column
	void FormatRow::CheckAndPersist() {

	}

}