#include "BACH/file/FileReaderCache.h"
#include "BACH/sstable/FileMetaData.h"
#include <iostream>
namespace BACH
{
	FileReaderCache::FileReaderCache(idx_t _max_size, std::string _prefix) :
		cache(_max_size), cache_deleting(_max_size),
		max_size(_max_size), prefix(_prefix),
		writing(std::make_shared<FileReader>("")) {}
	std::shared_ptr<FileReader> FileReaderCache::find(FileMetaData* file_data)
	{
		std::shared_ptr<FileReader> x;
		do
		{
			x = NULL;
			if (file_data->reader.compare_exchange_weak(x, writing))
			{
				file_data->reader.store(std::make_shared<FileReader>(
					prefix + file_data->file_name));
				idx_t pos;
				bool FALSE;
				do
				{
					do
					{
						pos = size.load();
					} while (!size.compare_exchange_weak(pos, (pos + 1) % max_size));
					FALSE = false;
				} while (!cache_deleting[pos].compare_exchange_weak(FALSE, true));
				if (cache[pos] != NULL)
				{
					cache[pos]->reader.store(NULL);
					cache[pos]->reader_pos = -1;
					if (cache[pos]->death)
						delete cache[pos];
				}
				cache[pos] = file_data;
				file_data->reader_pos = pos;
				cache_deleting[pos].store(false);
			}
			if (x == NULL || x == writing)
				continue;
			return x;
		} while (true);
	}
}
