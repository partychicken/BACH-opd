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
		PropertyFileParser(std::shared_ptr<FileReader> fr);
		~PropertyFileParser() = default;
		std::shared_ptr<std::string> GetProperty(idx_t index);

	private:
		std::shared_ptr<FileReader> reader;
		size_t count;
	};
}