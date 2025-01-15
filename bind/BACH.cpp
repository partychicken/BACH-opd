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
	void DB::AddVertexLabel(std::string label)
	{
		db->AddVertexLabel(label);
	}
	void DB::AddEdgeLabel(std::string label,
		std::string src_label, std::string dst_label)
	{
		db->AddEdgeLabel(label, src_label, dst_label);
	}
	void DB::CompactAll(double_t ratio)
	{
		db->CompactAll(ratio);
	}
	Transaction::Transaction(std::unique_ptr<BACH::Transaction> _txn) :
		txn(std::move(_txn)) {}
	Transaction::~Transaction() = default;
	vertex_t Transaction::AddVertex(label_t label)
	{
		return txn->AddVertex(label);
	}
	void Transaction::PutVertex(label_t label, vertex_t vertex_id, std::string_view property)
	{
		return txn->PutVertex(label, vertex_id, property);
	}
	std::shared_ptr<std::string> Transaction::GetVertex(vertex_t vertex, label_t label)
	{
		return txn->GetVertex(vertex, label);
	}
	void Transaction::DelVertex(vertex_t vertex, label_t label)
	{
		txn->DelVertex(vertex, label);
	}
	vertex_t Transaction::GetVertexNum(label_t label)
	{
		return txn->GetVertexNum(label);
	}

	void Transaction::PutEdge(vertex_t src, vertex_t dst,
		label_t label, edge_property_t property, bool delete_old)
	{
		txn->PutEdge(src, dst, label, property);
	}
	void Transaction::DelEdge(vertex_t src, vertex_t dst, label_t label)
	{
		txn->DelEdge(src, dst, label);
	}
	edge_property_t Transaction::GetEdge(
		vertex_t src, vertex_t dst, label_t label)
	{
		return txn->GetEdge(src, dst, label);
	}
	std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
		Transaction::GetEdges(vertex_t src, label_t label,
			const std::function<bool(edge_property_t&)>& func)
	{
		return txn->GetEdges(src, label, func);
	}
	void Transaction::EdgeLabelScan(label_t label,
		const std::function<void(vertex_t&, vertex_t&, edge_property_t&)>& func)
	{
		return txn->EdgeLabelScan(label, func);
	}
}