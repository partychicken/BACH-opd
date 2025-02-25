#include "BACH/file/FileReaderCache.h"
#include "BACH/sstable/FileMetaData.h"

namespace BACH
{
	FileReaderCache::FileReaderCache(idx_t _max_size, std::string _prefix) :
		cache(_max_size, NULL), cache_deleting(_max_size),
		max_size(_max_size), prefix(_prefix),
		writing(std::make_shared<FileReader>("")) {}
	std::shared_ptr<FileReader> FileReaderCache::find(FileMetaData* file_data)
	{
		std::shared_ptr<FileReader> x = NULL;
		while (x == NULL)
		{
			bool bo = false;
			if (file_data->reader_empty.compare_exchange_weak(bo, true))
			{
				std::shared_ptr<FileReader> new_reader = std::make_shared<FileReader>(
					prefix + file_data->file_name);
				file_data->reader = new_reader;
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
					cache[pos]->reader = NULL;
					cache[pos]->reader_pos = -1;
					cache[pos]->reader_empty.store(false);
				}
				cache[pos] = file_data;
				file_data->reader_pos = pos;
				cache_deleting[pos].store(false);
			}
			x = file_data->reader;
		}
		return x;
	}
	void FileReaderCache::deletecache(FileMetaData* file_data)
	{
		bool FALSE = false;
		while (file_data->reader_pos != (idx_t)-1 
			&&!cache_deleting[file_data->reader_pos].compare_exchange_strong(FALSE, true))
		{
			FALSE = false;
			std::this_thread::yield();
		}
		if(file_data->reader_pos == (idx_t)-1)
			return;
		cache[file_data->reader_pos] = NULL;
		cache_deleting[file_data->reader_pos].store(false);
	}
}
