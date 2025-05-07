#pragma once
#include "BACH/memory/RowMemoryManager.h"
#include "BACH/memory/TupleEntry.h"
#include "BACH/utils/ConcurrentArray.h"
#include <string>
#include <vector>

namespace BACH {

	// a class that use to temperary store the data from row format
	// technically don't have dictionary
	class TempColumn
	{
	public:
		TempColumn(relMemTable* row, size_t _size, size_t _column_num);
		~TempColumn();
		std::string* GetColumn(size_t column_id) {
			return data[column_id];
		}

	private:
		std::string** data;
		size_t size;
		size_t column_num;

	};

	// a class that use to store the data in column format
	// execute ap operator
	// get data from SSTable, using ordered dictionary
	class rowGroup {
	public:
		rowGroup();
		~rowGroup();

	private:


	};
	


} // namespace BACH