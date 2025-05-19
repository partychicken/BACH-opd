#include "BACH/memory/RowToColumn.h"

namespace BACH
{
	TempColumn::TempColumn(relMemTable* row, size_t _size, size_t _column_num) : size(_size), column_num(_column_num)
	{
		data = new std::string * [column_num];
		for (size_t i = 0; i < column_num; i++)
		{
			data[i] = new std::string[_size];
		}
		RelSkipList::Accessor accessor(row->tuple_index);
		idx_t count = 0;
		for (auto it = accessor.begin(); it != accessor.end(); ++it)
		{
			auto now_te = row->tuple_pool[it->second];
			if (now_te->property == TOMBSTONE || now_te->property == NONEINDEX)
			{
				for (size_t i = 0; i < column_num; i++)data[i][count] = "";
				count++;
				continue;
			}
			for (size_t i = 0; i < column_num; i++)
			{
				data[i][count] = now_te->tuple.row[i];
			}
			count++;
		}

	}

	TempColumn::~TempColumn()
	{
		for (size_t i = 0; i < column_num; i++)
		{
			delete[] data[i];
		}
		delete[] data;
	}

	rowGroup::rowGroup() {

	}
	rowGroup::~rowGroup() {

	}
}