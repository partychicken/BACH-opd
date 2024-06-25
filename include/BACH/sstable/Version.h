#pragma once

#include <vector>
#include "FileMetaData.h"

namespace BACH
{
	struct Version
	{
		std::vector<         //label
			std::vector<     //level
			std::shared_ptr<FileMetaData>>> files;
		idx_t refs = 0;
	};
}