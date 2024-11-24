#pragma once

#include <memory>
#include <string>
#include <vector>
#include "BACH/file/FileWriter.h"
#include "BACH/utils/utils.h"

namespace BACH
{
	class PropertyFileBuilder
	{
	public:
		PropertyFileBuilder(FileWriter* _filewriter);
		~PropertyFileBuilder() = default;
		void AddProperty(std::string_view property);
		void FinishFile();

	private:
		FileWriter* writer = nullptr;
		std::vector<size_t> offset_info;
		size_t offset = 0;
	};

}