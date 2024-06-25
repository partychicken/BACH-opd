#include "BACH/property/PropertyFileBuilder.h"

namespace BACH
{
	PropertyFileBuilder::PropertyFileBuilder(std::shared_ptr<FileWriter> _filewriter) :
		writer(_filewriter)
	{

	}
	void PropertyFileBuilder::AddProperty(std::string_view property)
	{
		writer->append(property.data(), property.size());
		offset += property.size();
		offset_info.push_back(offset);
	}
	void PropertyFileBuilder::FinishFile()
	{
		std::string temp_data;
		for (auto& i : offset_info)
			util::PutFixed(temp_data, i);
		util::PutFixed(temp_data, offset_info.size());
		writer->append(temp_data.data(), temp_data.size());
	}
}