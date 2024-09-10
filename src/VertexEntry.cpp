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
	SizeEntry::SizeEntry(vertex_t _begin_k, vertex_t _size) :
		begin_vertex_id(_begin_k* _size), entry(_size),
		immutable(false) {}
	void SizeEntry::delete_entry()
	{
		for (auto& memtable : entry)
			if (memtable != NULL)
				memtable->size_info.reset();
		for (auto& lmemtable : last->entry)
			if (lmemtable != NULL)
				lmemtable->next.reset();
		for (auto& i : entry)
			i.reset();
	}
	EdgeLabelEntry::EdgeLabelEntry(std::shared_ptr<Options> options,
		label_t src_label_id) :
		query_counter(options),
		//now_size_info(std::make_shared<SizeEntry>(0)),
		src_label_id(src_label_id),
		mutex() {}
}
//#endif // !VERTEXENTRY