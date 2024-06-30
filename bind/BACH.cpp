#include "BACH.h"
#include "BACH/BACH.h"

namespace bach
{
	DB::DB(std::shared_ptr<BACH::Options> _options) :
		db(std::make_unique<BACH::DB>(_options)) {}
	DB::~DB() = default;
	Transaction DB::BeginTransaction()
	{
		return std::make_unique<BACH::Transaction>(db->BeginTransaction());
	}
	Transaction DB::BeginReadOnlyTransaction()
	{
		return std::make_unique<BACH::Transaction>(db->BeginReadOnlyTransaction());
	}
	void DB::AddVertexLabel(std::string_view label)
	{
		db->AddVertexLabel(label);
	}
	void DB::AddEdgeLabel(std::string_view label,
		std::string_view src_label, std::string_view dst_label)
	{
		db->AddEdgeLabel(label, src_label, dst_label);
	}
	Transaction::Transaction(std::unique_ptr<BACH::Transaction> _txn) :
		txn(std::move(_txn)) {}
	Transaction::~Transaction() = default;
	vertex_t Transaction::AddVertex(label_t label, std::string_view property)
	{
		return txn->AddVertex(label, property);
	}
	std::shared_ptr<std::string> Transaction::FindVertex(vertex_t vertex, label_t label)
	{
		return txn->FindVertex(vertex, label);
	}
	void Transaction::PutEdge(vertex_t src, vertex_t dst,
		label_t label, edge_property_t property, bool delete_old)
	{
		txn->PutEdge(src, dst, label, property);
	}
	edge_property_t Transaction::FindEdge(
		vertex_t src, vertex_t dst, label_t label)
	{
		return txn->FindEdge(src, dst, label);
	}
	std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
		Transaction::FindEdges(
			label_t e_label, label_t s_label, label_t d_label, vertex_t src,
			bool (*func)(edge_property_t))
	{
		return txn->FindEdges(e_label, s_label, d_label, src, func);
	}
	bool Transaction::DelEdge(vertex_t src, vertex_t dst,
		label_t label)
	{
		return txn->DelEdge(src, dst, label);
	}
	void Transaction::DelVertex(vertex_t vertex, label_t label)
	{
		txn->DelVertex(vertex, label);
	}
	vertex_t Transaction::GetVertexNum(label_t label)
	{
		return txn->GetVertexNum(label);
	}
}