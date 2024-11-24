#pragma once

#include <memory>
#include <string>
#include <vector>
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"

namespace BACH
{
	class PropertyFileParser
	{
	public:
		PropertyFileParser(FileReader* fr);
		~PropertyFileParser() = default;
		std::shared_ptr<std::string> GetProperty(idx_t index);

	private:
		FileReader* reader;
		size_t count;
	};
}