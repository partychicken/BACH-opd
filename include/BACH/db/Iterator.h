#include <sul/dynamic_bitset.hpp>
#include "DB.h"
#include "Transaction.h"

namespace BACH
{
	class Iterator
	{
	public:
		Iterator(vertex_t _src, label_t _label, DB* _db, Transaction* _txn);
		~Iterator() = default;
		void Next();
		vertex_t GetNowDst() const { return now_dst; }
		edge_property_t GetNowProperty() const { return now_propertry; }
		bool IsValid() const { return valid; }

	private:
		vertex_t src;
		label_t label;
		DB* db;
		Transaction* txn;
		bool in_mem;
		bool valid;
		vertex_t now_dst;
		edge_property_t now_propertry;
		vertex_t now_pool_id;
		std::shared_ptr<VertexEntry> now_entry;
		VersionIterator iter;
		std::shared_ptr<SSTableParser> now_parser;
		bool parser_valid;
		sul::dynamic_bitset<> filter;
	};
}