#include "BACH/db/Transaction.h"

namespace BACH
{
	Transaction::Transaction(time_t epoch, DB* db,
		Version* _version, bool _read_only) :
		now_epoch(epoch), db(db), version(_version), read_only(_read_only) {}
	Transaction::~Transaction()
	{

	}

	vertex_t Transaction::AddVertex(label_t label, std::string_view property)
	{
		if (read_only)
		{
			return MAXVERTEX;
		}
		return db->Memtable->AddVertex(label, property, now_epoch);
	}
	std::shared_ptr<std::string> Transaction::GetVertex(
		vertex_t vertex, label_t label)
	{
		return db->Memtable->GetVertex(vertex, label, now_epoch);
	}
	void Transaction::DelVertex(vertex_t vertex, label_t label)
	{
		if (read_only)
		{
			return;
		}
		db->Memtable->DelVertex(vertex, label, now_epoch);
	}
	vertex_t Transaction::GetVertexNum(label_t label)
	{
		return db->Memtable->VertexLabelIndex.size();
	}

	void Transaction::PutEdge(vertex_t src, vertex_t dst, label_t label,
		edge_property_t property, bool delete_old = false)
	{
		if (read_only)
		{
			return;
		}
		db->Memtable->PutEdge(src, dst, label, property, now_epoch);

	}
}