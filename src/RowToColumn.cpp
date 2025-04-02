#include "BACH/memory/RowToColumn.h"

namespace BACH
{
	TempColumn::TempColumn(TupleIndexEntry row, size_t _size, size_t _column_num) : size(_size), column_num(_column_num)
	{
		data = new std::string * [column_num];
		for (size_t i = 0; i < size; i++)
		{
			data[i] = new std::string[_size];
		}
		RelSkipList::Accessor accessor(row.TPSizeIndex->tuple_index);
		for (auto it = accessor.begin(); it != accessor.end(); ++it)
		{
			auto key = it->first;
			auto index = it->second;
			auto tuple = row.TPSizeIndex->tuple_pool[index];
			if (tuple->property == NONEINDEX)
				continue;
			for (size_t i = 0; i < column_num; i++)
			{
				data[i][index] = tuple->tuple->GetRow(i);
			}
		}

	}

	TempColumn::~TempColumn()
	{
		for (size_t i = 0; i < size; i++)
		{
			delete[] data[i];
		}
		delete[] data;
	}

	FormatColumn::FormatColumn() {

	}
	FormatColumn::~FormatColumn() {

	}
}