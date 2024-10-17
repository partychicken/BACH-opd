#include "BACH/db/Iterator.h"

namespace BACH
{
	Iterator::Iterator(vertex_t _src, label_t _label, DB* _db, Transaction* _txn) :
		src(_src), label(_label), db(_db), txn(_txn), in_mem(true), valid(true),
		now_entry(NULL), iter(_txn->version, label, src), now_parser(NULL), parser_valid(false),
		filter(txn->GetVertexNum(label))
	{
		db->Memtable->EdgeLabelIndex[label]->query_counter.AddRead(src);
		auto k = src / db->options->MEMORY_MERGE_NUM;
		auto index = src - k * db->options->MEMORY_MERGE_NUM;
		std::shared_ptr<SizeEntry> size_entry;
		std::shared_ptr<VertexEntry> src_entry;
		do
		{
			size_entry = db->Memtable->EdgeLabelIndex[label]->SizeIndex[k];
		} while (size_entry == db->Memtable->writing_size);
		if (size_entry != NULL)
		{
			do
			{
				src_entry = size_entry->entry[index];
			} while (src_entry == db->Memtable->writing_vertex);
			now_entry = src_entry;
			now_pool_id = src_entry->EdgePool.size();
		}
		Next();
	}
	void Iterator::Next()
	{
		if (!valid)
			return;
		//still in memory
		if (in_mem)
		{
			//check if has left edge
		retry:
			while (now_pool_id == 0)
			{
				//if no left edge, check if has next entry
				if (now_entry->next == NULL)
				{
					//no next entry, finish work in memory, find in file
					in_mem = false;
					goto file;
				}
				//move to next entry
				now_entry = now_entry->next;
				now_pool_id = now_entry->EdgePool.size();
				//if this entry has no edge, continue to check next entry
			}
			//move to next edge
			std::shared_lock<std::shared_mutex>lock(now_entry->mutex);
			do
			{
				now_pool_id--;
			} while (now_pool_id >= 0 &&
				now_entry->EdgePool[now_pool_id].time > txn->read_epoch);
			if (now_pool_id < 0)
			{
				now_pool_id = 0;
				lock.unlock();
				goto retry;
			}
			now_dst = now_entry->EdgePool[now_pool_id].dst;
			now_propertry = now_entry->EdgePool[now_pool_id].property;
			lock.unlock();
			if(filter[now_dst])
				Next();
			else
				filter[now_dst] = true;
			return;
		}
	file:
		if (!parser_valid || !now_parser->GetNextEdgeBySrc())
		{
			while (!iter.End())
			{
				if ((*iter.GetFile()->filter)[src - iter.GetFile()->vertex_id_b])
				{
					auto fr = db->ReaderCaches->find(iter.GetFile());
					now_parser = std::make_shared<SSTableParser>(label, fr, db->options);
					if (now_parser->GetFirstEdgeBySrc(src))
					{
						parser_valid = true;
						break;
					}
				}
				iter.next();
			}
			if (iter.End())
			{
				valid = false;
				return;
			}
			iter.next();
		}
		now_dst = now_parser->GetNowEdgeDst();
		now_propertry = now_parser->GetNowEdgeProp();
		if (filter[now_dst])
			Next();
		else
			filter[now_dst] = true;
		return;
	}
}