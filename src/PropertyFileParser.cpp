#include "BACH/property/PropertyFileParser.h"

namespace BACH
{
	PropertyFileParser::PropertyFileParser(FileReader* fr) :
		reader(fr)
	{
		char buf[sizeof(size_t)];
		reader->rread(buf, sizeof(size_t), sizeof(size_t));
		util::DecodeFixed(buf, count);
	}
	std::shared_ptr<std::string> PropertyFileParser::GetProperty(idx_t index)
	{
		size_t len, offset;
		if (index == 0)
		{
			len = sizeof(size_t);
			offset = (count + 1) * sizeof(size_t);
		}
		else
		{
			len = sizeof(size_t) * 2;
			offset = (count + 2 - index) * sizeof(size_t);
		}
		char buf[len];
		reader->rread(buf, len, offset);
		if (index == 0)
		{
			offset = 0;
			util::DecodeFixed(buf, len);
		}
		else
		{
			util::DecodeFixed(buf, offset);
			util::DecodeFixed(buf + sizeof(size_t), len);
			len -= offset;
		}
		auto property=std::make_shared<std::string>();
		property->resize(len);
		reader->fread(property->data(), len, offset);
		return property;
	}
}