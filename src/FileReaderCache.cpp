#include "BACH/file/FileReaderCache.h"
#include "BACH/sstable/FileMetaData.h"

namespace BACH
{
	FileReaderCache::FileReaderCache(idx_t _max_size, std::string _prefix) :
		cache(_max_size, NULL), cache_deleting(_max_size),
		max_size(_max_size), prefix(_prefix), no_cache(_max_size == 0) {}
		
	FileReader* FileReaderCache::find(FileMetaData* file_data)
	{
		FileReader* x = NULL;
		while (x == NULL || x == (FileReader *)-1)
		{
			x = NULL;
			if (file_data->reader.compare_exchange_weak(x, (FileReader *)-1))
			{
				std::string name = prefix + file_data->file_name;
				file_data->reader.store(new FileReader(name, file_data->id));
				if(!no_cache)
				{
					idx_t pos;
					bool FALSE;
				retry:
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
						auto tmp = cache[pos]->reader.load();
						if(tmp == (FileReader *)-1)
							goto retry;
						cache[pos]->reader.store(NULL);
						tmp->DecRef();
						cache[pos]->reader_pos = -1;
					}
					cache[pos] = file_data;
					file_data->reader_pos = pos;
					cache_deleting[pos].store(false);
				}
			}
			x = file_data->reader;
		}
		if(!x->AddRef())
			return find(file_data);
		if(x->get_id() != file_data->id)
		{
			x->DecRef();
			return find(file_data);
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
