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
#include "BACH/memory/AnswerMerger.h"

namespace BACH {
    struct TupleEntry {
        TupleEntry(Tuple _tuple, time_t _time, bool tombstone = false, idx_t _next = NONEINDEX) : 
            tuple(_tuple), time(_time), tombstone(tombstone), next(_next) {
        }

        TupleEntry(std::string key, time_t time) : time(time) {
            tuple.row.push_back(key);
        }

        Tuple tuple;
        time_t time = MAXTIME; 
        bool tombstone = false;
        idx_t next = NONEINDEX;

        bool operator<(const TupleEntry &other) const {
            return tuple.GetKey() == other.tuple.GetKey() ? time > other.time : tuple.GetKey() < other.tuple.GetKey();
        }
    };




    // currently use string as key
    typedef std::string tp_key;


    // string comparator,used in SkipList
    //struct ReverseCompare {
    //	bool operator()(const std::pair<tp_key, idx_t>& a, const std::pair<tp_key, idx_t>& b) const {
    //		return a.first <  b.first;
    //	}
    //};


    // compare key first, then time
    struct ReverseCompare {
        bool operator()(const std::pair<tp_key, idx_t>&a,
                        const std::pair<tp_key, idx_t>&b) const {
            return a.first < b.first;
        }
    };


    typedef folly::ConcurrentSkipList<std::pair<tp_key, idx_t>, ReverseCompare> RelSkipList;

	struct relMemTable
	{
		// metadata for tuples
		tp_key max_key;
		tp_key min_key;
		std::atomic<size_t> total_tuple;
		idx_t column_num;
        size_t current_data_count = 0; // ��ǰ����������
		
		
		std::shared_ptr<RelSkipList> tuple_index;
		std::shared_mutex mutex;
		ConcurrentArray<TupleEntry *> tuple_pool;
		std::shared_ptr<relMemTable> last = NULL;
		std::weak_ptr<relMemTable> next;
		std::atomic<bool> immutable;
		std::counting_semaphore<1024> sema;
		std::map <tp_key, time_t> del_table;
		size_t size = 0;
		time_t max_time = 0;
		relMemTable(size_t _size, idx_t _column_num,
			std::shared_ptr<relMemTable> _next = NULL) :
			total_tuple(_size), column_num(_column_num), tuple_pool(20), 
            next(_next), immutable(false), sema(0) {
			tuple_index = RelSkipList::createInstance();
		};
		~relMemTable() {
            for (size_t i = 0; i < tuple_pool.size(); ++i) {
                delete tuple_pool[i];
            }
		}
		void UpdateMinMax(tp_key key);

        bool PutTuple(Tuple tuple, tp_key key, time_t timestamp, bool tombstone = false);

        //void AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
        //void DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property);
        Tuple GetTuple(tp_key key, time_t timestamp);

        //void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
        std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);

        void FilterByValueRange(time_t timestamp, const std::function<bool(Tuple &)> &func, AnswerMerger &am);

		void GetKTuple(idx_t k, tp_key key, time_t timestamp, std::vector<std::unique_ptr<Tuple>> &am);
    };


    // refer to EdgeLabelEntry

    //memtable
    //struct TupleIndexEntry
    //{
    //	// original sizeindex store different label separately
    //	// tuple_index = tuple[], TPSizeIndex is a block of tuple(the block number can be set in option)
    //	// TPSizeIndex currently don't have label, use below to store
    //	// ConcurrentArray <std::shared_ptr<TPSizeEntry>> TPSizeIndex;
    //	std::shared_ptr<relMemTable> TPSizeIndex;

    //	// maybe useful for future, save for now
    //	QueryCounter query_counter;

    //	std::shared_mutex mutex;
    //	TupleIndexEntry(std::shared_ptr<Options> options) :  query_counter(options){
    //		TPSizeIndex = std::make_shared<relMemTable>(0);
    //	};

    //	void AddTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
    //	void DeleteTuple(tp_key key, time_t timestamp, tuple_property_t property);
    //	Tuple GetTuple(tp_key key, time_t timestamp);
    //	void UpdateTuple(Tuple tuple, tp_key key, time_t timestamp, tuple_property_t property);
    //	std::vector<Tuple> ScanTuples(tp_key start_key, tp_key end_key, time_t timestamp);
    //};
}
