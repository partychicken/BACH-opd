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
		tp_key last_key = "";
		idx_t count = 0;
		for (auto it = accessor.begin(); it != accessor.end(); ++it)
		{
			auto now_key = it->tuple.row[0];
			if (now_key == last_key)
				continue;

			auto tuple = *it;
			if (tuple.property == TOMBSTONE || tuple.property == NONEINDEX)
			{
				for (size_t i = 0; i < column_num; i++)data[i][count] = "";
				count++;
				last_key = now_key;
				continue;
			}
			for (size_t i = 0; i < column_num; i++)
			{
				data[i][count] = tuple.tuple.GetRow(i);
			}
			count++;
			last_key = now_key;
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