#include "BACH/memory/VertexEntry.h"

namespace BACH
{
	VertexEntry::VertexEntry(SizeEntry* size_info, VertexEntry* _next) :
		EdgeIndex(), mutex(), next(_next)
	{
		this->size_info = size_info;
		size_info->entry.push_back(this);
		if (_next != NULL)
			deadtime = _next->deadtime;
	}
	SizeEntry::SizeEntry(vertex_t _begin_vertex_id, SizeEntry* next) :
		begin_vertex_id(_begin_vertex_id), immutable(false)
	{
		if (next != NULL)
			next->last = this;
	}
	void SizeEntry::delete_entry()
	{
		for (auto& memtable : last->entry)
		{
			memtable->next = NULL;
		}
		for (auto& verentry : entry)
		{
			verentry->mutex.lock();
			delete verentry;
		}
		delete this;
	}
	EdgeLabelEntry::EdgeLabelEntry(label_t src_label_id, size_t list_num) :
		query_counter(list_num),
		now_size_info(new SizeEntry(0, NULL)),
		src_label_id(src_label_id),
		mutex() {}
}
//#endif // !VERTEXENTRY