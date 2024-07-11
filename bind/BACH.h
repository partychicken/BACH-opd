#include <string>
#include <memory>
#include <vector>

#pragma once

namespace BACH
{
	using vertex_t = uint32_t;
	using idx_t = int32_t;
#ifndef OPTION
#define OPTION
	struct Options
	{
		std::string STORAGE_DIR = "./output/db_storage";
		size_t MEM_TABLE_MAX_SIZE = 4 * 1024;
		size_t VERTEX_PROPERTY_MAX_SIZE = 64 * 1024 * 1024;
		vertex_t MERGE_NUM = 32;
		vertex_t MIN_MERGE_NUM = 20;
		size_t READ_BUFFER_SIZE = 1024 * 1024;
		size_t WRITE_BUFFER_SIZE = 1024 * 1024;
		size_t NUM_OF_COMPACTION_THREAD = 1;
		bool DELETE_UNUSED_FILE = true;
		double FALSE_POSITIVE = 0.01;
	};
#else
	struct Options;
#endif
	class DB;
	class Transaction;
}

namespace bach
{
	using label_t = uint16_t;
	using vertex_t = uint32_t;
	using edge_property_t = double_t;

	class Transaction;

	class DB
	{
	public:
		DB(std::shared_ptr<BACH::Options> _options);
		~DB();
		Transaction BeginWriteTransaction();
		Transaction BeginReadTransaction();
		void AddVertexLabel(std::string label_name);
		void AddEdgeLabel(std::string label_name,
			std::string src_label_name, std::string dst_label_name);
	private:
		const std::unique_ptr<BACH::DB> db;
	};
	class Transaction
	{
	public:
		Transaction(std::unique_ptr<BACH::Transaction> _txn);
		~Transaction();
		vertex_t AddVertex(label_t label, std::string_view property);
		std::shared_ptr<std::string> GetVertex(vertex_t vertex, label_t label);
		void DelVertex(vertex_t vertex, label_t label);
		vertex_t GetVertexNum(label_t label);

		void PutEdge(vertex_t src, vertex_t dst, label_t label,
			edge_property_t property, bool delete_old = false);
		void DelEdge(vertex_t src, vertex_t dst, label_t label);
		edge_property_t GetEdge(
			vertex_t src, vertex_t dst, label_t label);
		std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
			GetEdges(vertex_t src, label_t label,
				bool (*func)(edge_property_t) = [](edge_property_t x) {return true; });

	private:
		const std::unique_ptr<BACH::Transaction> txn;
	};
}