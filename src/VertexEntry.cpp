#include "BACH/memory/VertexEntry.h"

namespace BACH
{
	VertexEntry::VertexEntry(std::shared_ptr < SizeEntry > size_info,
		std::shared_ptr < VertexEntry > _next) :
		EdgeIndex(), mutex(), next(_next)
	{
		this->size_info = size_info;
		if (_next != NULL)
			deadtime = _next->deadtime;
	}
	SizeEntry::SizeEntry(vertex_t _begin_vertex_id) :
		begin_vertex_id(_begin_vertex_id), immutable(false) {}
	void SizeEntry::delete_entry()
	{
		for (auto& memtable : entry)
		{
			memtable->size_info.reset();
		}
		for (auto& lmemtable : last->entry)
		{
			lmemtable->next.reset();
		}
		for (auto& i : entry)
			i.reset();
	}
	EdgeLabelEntry::EdgeLabelEntry(label_t src_label_id, size_t list_num) :
		query_counter(list_num),
		now_size_info(std::make_shared<SizeEntry>(0)),
		src_label_id(src_label_id),
		mutex() {}
}
//#endif // !VERTEXENTRY