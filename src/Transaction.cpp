#include "BACH/db/Transaction.h"

namespace BACH
{
	vertex_t Transaction::AddVertex(label_t label, std::string_view property)
	{
		if (read_only)
		{
			return MAXVERTEX;
		}
		return db->Memtable->AddVertex(label, property, now_epoch);
	}
	std::shared_ptr<std::string> Transaction::FindVertex(
		vertex_t vertex, label_t label)
	{
		return db->Memtable->FindVertex(vertex, label, now_epoch);
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

	}
}