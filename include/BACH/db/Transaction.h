#pragma once

#include <string>
#include <unordered_map>
#include "DB.h"
#include "BACH/sstable/SSTableParser.h"
#include "BACH/common/tuple.h"
#include "BACH/sstable/RelVersion.h"
#include "BACH/sstable/FileMetaData.h"
#include "BACH/sstable/BloomFilter.h"
#include "BACH/memory/RowMemoryManager.h"
#include "BACH/memory/RowGroup.h"
#include "BACH/memory/AnswerMerger.h" 
#include "BACH/profiler/profiler.h"
#include "BACH/profiler/OperatorProfilerContext.h"

namespace BACH
{
	class Transaction
	{
	public:
		Transaction() = delete;
		Transaction& operator=(const Transaction&) = delete;
		Transaction(const Transaction& txn) = delete;
		Transaction(time_t _write_epoch, time_t _read_epoch,
			DB* db, Version* _version, time_t pos = -1);

		Transaction(time_t _write_epoch, time_t _read_epoch,
			DB* db, RelVersion* _version, time_t pos = -1);

		Transaction(Transaction&& txn);
		~Transaction();

		//vertex operation
		vertex_t AddVertex(label_t label);
		void PutVertex(label_t label, vertex_t vertex_id, std::string_view property);
		std::shared_ptr<std::string> GetVertex(vertex_t vertex, label_t label);
		void DelVertex(vertex_t vertex, label_t label);
		vertex_t GetVertexNum(label_t label);

		//edge operation
		void PutEdge(vertex_t src, vertex_t dst, label_t label,
			edge_property_t property, bool delete_old = false);
		void DelEdge(vertex_t src, vertex_t dst, label_t label);
		edge_property_t GetEdge(
			vertex_t src, vertex_t dst, label_t label);
		std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
			GetEdges(vertex_t src, label_t label,
				const std::function<bool(edge_property_t&)>& func
				= [](edge_property_t x) {return true; });
		//only work when there is only one version for any edge
		void EdgeLabelScan(label_t label,
			const std::function<void(vertex_t&, vertex_t&,edge_property_t&)>& func);
		// OLTP operation
        void PutTuple(Tuple tuple, tp_key key);
        void DelTuple(tp_key key);
        Tuple GetTuple(tp_key key);


        // OLAP operation
		void GetTuplesFromRange(idx_t col_id, std::string left_bound, std::string right_bound) {
			OperatorProfiler op;
			OperatorProfilerContext::SetCurrentProfiler(&op);
			op.Start();

			AnswerMerger am;
			// add in-memory data into am here;
			auto vf = CreateValueFilterFunction(col_id, left_bound, right_bound);
			db->RowMemtable->FilterByValueRange(read_epoch, vf, am, rel_version);

			auto files = rel_version->FileIndex;

			//auto MakeLeftBoundFunc = [](const std::string& right_bound) {
			//	// ����һ������������� lambda����Ϊ right_bound ��ֵ���񣬱հ�������Ȼ�Ǻ�������
			//	return [=](const std::string& s) {
			//		return s < right_bound;
			//		};
			//	};

			//auto left_func = MakeLeftBoundFunc(left_bound);

			//auto right_func = MakeLeftBoundFunc(right_bound);

			for (auto cur_level : files) {
				for (auto cur_file : cur_level) {
					//auto reader = db->ReaderCaches->find(cur_file);
					//auto parser = RelFileParser<std::string>(reader, db->options, cur_file->file_size);
					//auto DictList = static_cast<RelFileMetaData<std::string> *>(cur_file)->dictionary;
					RowGroup cur_row_group(db, static_cast<RelFileMetaData<std::string> *>(cur_file));
					cur_row_group.GetKeyData();
					cur_row_group.GetAllColData();
					// minus 1 because the first column is key
					cur_row_group.ApplyRangeFilter(col_id - 1, left_bound, right_bound, am);
				}
			}

			op.End();
			profiler.AddOperator("GetTuplesFromRange", op);
			OperatorProfilerContext::SetCurrentProfiler(nullptr);
		}

        void ScanTuples();

		std::vector<Tuple> ScanKTuples(idx_t k, std::string key);

	private:
		time_t write_epoch;
		time_t read_epoch;
		DB* db;
		size_t time_pos;

		Version* version;
		RelVersion* rel_version;
		ThreadProfiler profiler;
		bool valid = true;


	};
}
