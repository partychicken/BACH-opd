#include "BACH/memory/VertexEntry.h"

namespace BACH
{
	SizeEntry::SizeEntry(vertex_t _begin_k, vertex_t _size,
		std::shared_ptr<SizeEntry>_next) :
		begin_vertex_id(_begin_k* _size), edge_index(_size), mutex(_size),
		next(_next), immutable(false), sema(0) {}
	void SizeEntry::delete_entry()
	{
		last->next = NULL;
	}
	EdgeLabelEntry::EdgeLabelEntry(std::shared_ptr<Options> options,
		label_t src_label_id) :
		query_counter(options),
		src_label_id(src_label_id),
		mutex() {}
}
//#endif // !VERTEXENTRY