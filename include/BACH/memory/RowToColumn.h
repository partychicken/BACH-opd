#pragma once
#include "BACH/memory/FormatRow.h"
#include "BACH/memory/TupleEntry.h"
#include "BACH/utils/ConcurrentArray.h"

namespace BACH {

	// a class that use to temperary store the data from row format
	// technically don't have dictionary
	class TempColumn
	{
	public:
		TempColumn(TupleIndexEntry row, size_t _size, size_t _column_num);
		~TempColumn();

	private:
		std::string** data;
		size_t size;
		size_t column_num;

	};

	// a class that use to store the data in column format
	// execute ap operator
	// get data from SSTable, using ordered dictionary
	class FormatColumn {
	public:
		FormatColumn();
		~FormatColumn();

	private:


	};
	


} // namespace BACH