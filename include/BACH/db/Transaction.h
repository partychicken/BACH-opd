#pragma once

#include "DB.h"
#include "BACH/sstable/SSTableParser.h"

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

	private:
		time_t write_epoch;
		time_t read_epoch;
		DB* db;
		Version* version;
		size_t time_pos;
		bool valid = true;
	};
}