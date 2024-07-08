#include "BACH/db/Transaction.h"

namespace BACH
{
	Transaction::Transaction(time_t epoch, DB* db,
		Version* _version, bool _read_only) :
		now_epoch(epoch), db(db), version(_version), read_only(_read_only) {}
	Transaction::~Transaction()
	{
		if (read_only)
		{
			std::unique_lock<std::shared_mutex> rlock(db->read_epoch_table_mutex);
			if (--db->read_epoch_table[now_epoch] == 0)
			{
				db->read_epoch_table.erase(now_epoch);
				db->Memtable->ClearDelTable(now_epoch);
			}
			
		}
		else
		{
			std::unique_lock<std::shared_mutex> wlock(db->write_epoch_table_mutex);
			db->write_epoch_table.erase(now_epoch);
		}
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