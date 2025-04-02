#pragma once

#include <atomic>
#include <memory>
#include <semaphore>
#include <shared_mutex>
#include <vector>
#include <folly/ConcurrentSkipList.h>
#include "QueryCounter.h"
#include "BACH/utils/types.h"
#include "BACH/utils/ConcurrentArray.h"
#include "BACH/common/tuple.h"

namespace BACH 
{

	struct TupleEntry {
		std::shared_ptr<Tuple> tuple;
		time_t time;
		// function as a signal whether this record has been deleted
		// don't know the reason to use double but not bool
		tuple_property_t property;
		tuple_t next;
		TupleEntry(std::shared_ptr<Tuple> _tuple, time_t _time, 
					tuple_property_t _property, tuple_t _next = NONEINDEX) : 
			tuple(_tuple), time(_time), next(_next), property(_property){};
	};


	// currently use string as key
	typedef std::string tp_key;


	// string comparator,used in SkipList
	struct ReverseCompare {
		bool operator()(const std::pair<tp_key, idx_t>& a, const std::pair<tp_key, idx_t>& b) const {
			return a.first <  b.first;
		}
	};


	typedef folly::ConcurrentSkipList<std::pair<tp_key, idx_t>, ReverseCompare> RelSkipList;
	struct TPSizeEntry
	{
		// metadata for tuples
		tp_key max_key;
		tp_key min_key;
		
		std::atomic<size_t> total_tuple;
		std::shared_ptr<RelSkipList> tuple_index;
		std::shared_mutex mutex;
		ConcurrentArray<std::shared_ptr<TupleEntry>> tuple_pool;
		std::shared_ptr<TPSizeEntry> last = NULL, next = NULL;
		std::atomic<bool> immutable;
		std::counting_semaphore<1024> sema;
		std::map <tp_key, time_t> del_table;
		/*size_t size = 0;*/
		time_t max_time = 0;
		TPSizeEntry(size_t _size,
			std::shared_ptr<TPSizeEntry> _next = NULL) :
			total_tuple(_size), next(_next), immutable(false), sema(0) {
			tuple_index = RelSkipList::createInstance();
		};
		void delete_entry() {
			last->next = NULL;
		};

		void AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
		void DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property);
		Tuple GetTuple(tp_key key, time_t timestamp);
		void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
		std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);



	}; 




	// refer to EdgeLabelEntry
	struct TupleIndexEntry
	{
		// original sizeindex store different label separately 
		// tuple_index = tuple[], TPSizeIndex is a block of tuple(the block number can be set in option)
		// TPSizeIndex currently don't have label, use below to store
		// ConcurrentArray <std::shared_ptr<TPSizeEntry>> TPSizeIndex;
		std::shared_ptr<TPSizeEntry> TPSizeIndex;

		// maybe useful for future, save for now
		// TODO: revise QueryCounter
		QueryCounter query_counter;
		std::shared_mutex mutex;
		TupleIndexEntry(std::shared_ptr<Options> options) :  query_counter(options){
			TPSizeIndex = std::make_shared<TPSizeEntry>(0);
		};

		void AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
		void DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property);
		Tuple GetTuple(tp_key key, time_t timestamp);
		void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
		std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);


	};

}