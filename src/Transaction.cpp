#include "BACH/db/Transaction.h"

namespace BACH
{
	Transaction::Transaction(time_t epoch, DB* db,
		Version* _version, bool _read_only) :
		now_epoch(epoch), db(db), version(_version), read_only(_read_only)
	{
		version->AddRef();
	}
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
			version->DecRef();
		}
		else
		{
			std::unique_lock<std::shared_mutex> wlock(db->write_epoch_table_mutex);
			db->write_epoch_table.erase(now_epoch);
			wlock.unlock();
			db->ProgressReadVersion();
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
		return db->Memtable->GetVertexNum(label);
	}

	void Transaction::PutEdge(vertex_t src, vertex_t dst, label_t label,
		edge_property_t property, bool delete_old)
	{
		if (read_only)
		{
			return;
		}
		//if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
		//{
		//	std::cout<<"add edge from a deleted vertex!\n";
		//}
		db->Memtable->PutEdge(src, dst, label, property, now_epoch);
	}
	void Transaction::DelEdge(vertex_t src, vertex_t dst, label_t label)
	{
		if (read_only)
		{
			return;
		}
		//if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
		//{
		//	std::cout << "delete edge from a deleted vertex!\n";
		//}
		db->Memtable->DelEdge(src, dst, label, now_epoch);
	}
	edge_property_t Transaction::GetEdge(
		vertex_t src, vertex_t dst, label_t label)
	{
		//if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
		//{
		//	std::cout << "find edge from a deleted vertex!\n";
		//}
		edge_property_t x = db->Memtable->GetEdge(src, dst, label, now_epoch);
		if (x != TOMBSTONE)
			return x;
		VersionIterator iter(version, label, src);
		while (!iter.End())
		{
			auto fr = std::make_shared<FileReader>(
				db->options->STORAGE_DIR + "/" + iter.GetFile()->file_name);
			auto parser = std::make_shared<SSTableParser>(label, fr, db->options);
			auto found = parser->GetEdge(src, dst);
			if (found != TOMBSTONE)
				return found;
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
		db->Memtable->GetEdges(src, label, now_epoch, answer_temp, func);
		VersionIterator iter(version, label, src);
		while (!iter.End())
		{
			auto fr = std::make_shared<FileReader>(
				db->options->STORAGE_DIR + "/" + iter.GetFile()->file_name);
			auto parser = std::make_shared<SSTableParser>(label, fr, db->options);
			parser->GetEdges(src, answer_temp, func);
		}
		std::sort(answer_temp->begin(), answer_temp->end());
		auto last = std::unique(answer_temp->begin(), answer_temp->end(),
			[](const std::tuple<vertex_t, vertex_t, edge_property_t>& A,
				const std::tuple<vertex_t, vertex_t, edge_property_t>& B)
			{
				return std::get<0>(A) == std::get<0>(B);
			});
		answer_temp->erase(last, answer_temp->end());
		for (auto& i : *answer_temp)
		{
			if (std::get<2>(i) != TOMBSTONE)
			{
				(*answer).emplace_back(std::get<0>(i), std::get<2>(i));
			}
		}
		return answer;
	}
}