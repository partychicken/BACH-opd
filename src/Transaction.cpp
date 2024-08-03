#include "BACH/db/Transaction.h"

namespace BACH
{
	Transaction::Transaction(time_t _write_epoch, time_t _read_epoch,
		DB* db, Version* _version) :
		write_epoch(_write_epoch), read_epoch(_read_epoch), db(db), version(_version)
	{
		version->AddRef();
	}
	Transaction::Transaction(Transaction&& txn) :
		write_epoch(txn.write_epoch), read_epoch(txn.read_epoch),
		db(txn.db), version(txn.version)
	{
		txn.valid = false;
	}
	Transaction::~Transaction()
	{
		if (valid)
		{
			{
				std::unique_lock<std::shared_mutex> rlock(db->read_epoch_table_mutex);
				if (--db->read_epoch_table[read_epoch] == 0)
					db->read_epoch_table.erase(read_epoch);
				version->DecRef();
			}
			if (write_epoch != MAXTIME)
			{
				std::unique_lock<std::shared_mutex> wlock(db->write_epoch_table_mutex);
				db->write_epoch_table.erase(write_epoch);
				wlock.unlock();
				db->ProgressReadVersion();
			}
		}
	}

	vertex_t Transaction::AddVertex(label_t label, std::string_view property)
	{
		if (write_epoch == MAXTIME)
		{
			return MAXVERTEX;
		}
		return db->Memtable->AddVertex(label, property, write_epoch);
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
			auto fr = std::make_shared<FileReader>(
				db->options->STORAGE_DIR + "/" + iter.GetFile()->file_name);
			auto parser = std::make_shared<SSTableParser>(label, fr, db->options);
			//std::cout << "get edge " + std::to_string(src) + "to" + std::to_string(dst)
			//	+ " from file " + iter.GetFile()->file_name + "\n";
			auto found = parser->GetEdge(src, dst);
			if (!std::isnan(found))
				return found;
			iter.next();
		}
		return TOMBSTONE;
	}
	std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t>>>
		Transaction::GetEdges(vertex_t src, label_t label,
			bool (*func)(edge_property_t))
	{
		//<dst,index,property>
		auto answer_temp = std::make_shared<std::vector<
			std::tuple<vertex_t, vertex_t, edge_property_t>>>();
		auto answer = std::make_shared<std::vector<
			std::pair<vertex_t, edge_property_t>>>();
		db->Memtable->GetEdges(src, label, read_epoch, answer_temp, func);
		VersionIterator iter(version, label, src);
		while (!iter.End())
		{
			auto fr = std::make_shared<FileReader>(
				db->options->STORAGE_DIR + "/" + iter.GetFile()->file_name);
			auto parser = std::make_shared<SSTableParser>(label, fr, db->options);
			parser->GetEdges(src, answer_temp, func);
			iter.next();
		}
		std::sort(answer_temp->begin(), answer_temp->end());
		auto last = std::unique(answer_temp->begin(), answer_temp->end(),
			[](const std::tuple<vertex_t, vertex_t, edge_property_t>& A,
				const std::tuple<vertex_t, vertex_t, edge_property_t>& B)
			{
				return std::get<0>(A) == std::get<0>(B);
			});
		answer_temp->erase(last, answer_temp->end());
		for (auto i = answer_temp->begin(); i != last; i++)
			if (std::get<2>(*i) != TOMBSTONE)
				(*answer).emplace_back(std::get<0>(*i), std::get<2>(*i));
		return answer;
	}
}