#include "BACH/db/Transaction.h"

namespace BACH
{
	Transaction::Transaction(time_t _write_epoch, time_t _read_epoch,
		DB* db, Version* _version, time_t pos) :
		write_epoch(_write_epoch), read_epoch(_read_epoch), db(db), version(_version), time_pos(pos) {}
	Transaction::Transaction(Transaction&& txn) :
		write_epoch(txn.write_epoch), read_epoch(txn.read_epoch),
		db(txn.db), version(txn.version), time_pos(txn.time_pos)
	{
		txn.valid = false;
	}
	Transaction::~Transaction()
	{
		if (valid)
		{
			version->DecRef();
			if (write_epoch != MAXTIME)
			{
				db->write_epoch_table.erase(time_pos);
				db->ProgressReadVersion();
			}
		}
	}

	vertex_t Transaction::AddVertex(label_t label)
	{
		if (write_epoch == MAXTIME)
		{
			return MAXVERTEX;
		}
		return db->Memtable->AddVertex(label);
	}
	void Transaction::PutVertex(label_t label, vertex_t vertex_id, std::string_view property)
	{
		if (write_epoch == MAXTIME)
		{
			return;
		}
		db->Memtable->PutVertex(label, vertex_id, property);
	}
	std::shared_ptr<std::string> Transaction::GetVertex(
		vertex_t vertex, label_t label)
	{
		return db->Memtable->GetVertex(vertex, label, read_epoch);
	}
	void Transaction::DelVertex(vertex_t vertex, label_t label)
	{
		if (write_epoch == MAXTIME)
		{
			return;
		}
		db->Memtable->DelVertex(vertex, label, write_epoch);
	}
	vertex_t Transaction::GetVertexNum(label_t label)
	{
		return db->Memtable->GetVertexNum(label);
	}

	void Transaction::PutEdge(vertex_t src, vertex_t dst, label_t label,
		edge_property_t property, bool delete_old)
	{
		if (write_epoch == MAXTIME)
		{
			return;
		}
		//if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
		//{
		//	std::cout<<"add edge from a deleted vertex!\n";
		//}
		db->Memtable->PutEdge(src, dst, label, property, write_epoch);
	}
	void Transaction::DelEdge(vertex_t src, vertex_t dst, label_t label)
	{
		if (write_epoch == MAXTIME)
		{
			return;
		}
		//if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
		//{
		//	std::cout << "delete edge from a deleted vertex!\n";
		//}
		db->Memtable->DelEdge(src, dst, label, write_epoch);
	}
	edge_property_t Transaction::GetEdge(
		vertex_t src, vertex_t dst, label_t label)
	{
		edge_property_t x = db->Memtable->GetEdge(src, dst, label, read_epoch);
		if (!std::isnan(x))
			return x;
		VersionIterator iter(version, label, src);
		while (!iter.End())
		{
			if (src - iter.GetFile()->vertex_id_b < iter.GetFile()->filter->size())
				if ((*iter.GetFile()->filter)[src - iter.GetFile()->vertex_id_b])
				{
					SSTableParser parser(label, db->ReaderCaches->find(iter.GetFile()), db->options);
					auto found = parser.GetEdge(src, dst);
					if (!std::isnan(found))
						return found;
				}
			iter.next();
		}
		return TOMBSTONE;
	}
	std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
		Transaction::GetEdges(vertex_t src, label_t label,
			const std::function<bool(edge_property_t&)>& func)
	{
		std::shared_ptr<std::vector<std::pair<
			vertex_t, edge_property_t>>> answer_temp[3];
		vertex_t c = 0;
		for (size_t i = 0; i < 3; i++)
			answer_temp[i] = std::make_shared<std::vector<
			std::pair<vertex_t, edge_property_t>>>();
		db->Memtable->GetEdges(src, label, read_epoch, answer_temp, c, func);
		VersionIterator iter(version, label, src);
		while (!iter.End())
		{
			if (src - iter.GetFile()->vertex_id_b < iter.GetFile()->filter->size())
				if ((*iter.GetFile()->filter)[src - iter.GetFile()->vertex_id_b])
				{
					SSTableParser parser(label, db->ReaderCaches->find(iter.GetFile()), db->options);
					parser.GetEdges(src, answer_temp[(c + 1) % 3], func);
					db->Memtable->merge_answer(answer_temp, c);
				}
			iter.next();
		}
		return answer_temp[c];
	}
	void Transaction::EdgeLabelScan(
		label_t label, const std::function<void(vertex_t&, vertex_t&, edge_property_t&)>& func)
	{
		db->Memtable->EdgeLabelScan(label, func);
		if (version->FileIndex.size() <= label)
			return;
		for (auto& i : version->FileIndex[label])
			for (auto& j : i)
			{
				SSTableParser parser(label, db->ReaderCaches->find(j), db->options);
				if (!parser.GetFirstEdge())
					continue;
				do
				{
					auto src = parser.GetNowEdgeSrc();
					auto dst = parser.GetNowEdgeDst();
					auto prop = parser.GetNowEdgeProp();
					func(src, dst, prop);
				} while (parser.GetNextEdge());
			}
	}
}