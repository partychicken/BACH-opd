#pragma once

#include "DB.h"
#include "BACH/utils/types.h"

#ifndef TransactionOnce
#define TransactionOnce

namespace BACH
{
	class DB;
	class Transaction
	{
	public:
		Transaction(time_t epoch, DB* db, bool _read_only);
		~Transaction();

		//vertex operation
		vertex_t AddVertex(label_t label, std::string_view property);
		std::shared_ptr<std::string> FindVertex(vertex_t vertex, label_t label);
		void DelVertex(vertex_t vertex, label_t label);
		vertex_t GetVertexNum(label_t label);

		//edge operation
		void PutEdge(vertex_t src, vertex_t dst, label_t label,
			edge_property_t property, bool delete_old = false,
			bool tombstone = false);
		bool DelEdge(vertex_t src, vertex_t dst, label_t label);
		edge_property_t FindEdge(
			vertex_t src, vertex_t dst, label_t label);
		std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
			FindEdges(
				label_t e_label, label_t s_label, label_t d_label, vertex_t src,
				bool (*func)(edge_property_t) = [](edge_property_t x) {return true; });

	private:
		time_t now_epoch;
		DB* db;
		bool read_only;
	};
}

#endif