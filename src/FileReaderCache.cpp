#include "BACH/file/FileReaderCache.h"
#include "BACH/sstable/FileMetaData.h"
#include <iostream>
namespace BACH
{
	FileReaderCache::FileReaderCache(idx_t _max_size, std::string _prefix) :
		cache(_max_size), cache_deleting(_max_size),
		max_size(_max_size), prefix(_prefix) {}
	FileReader* FileReaderCache::find(FileMetaData* file_data)
	{
		FileReader* x = NULL;
		if (file_data->reader.compare_exchange_weak(x, (FileReader*)1))
		{
			file_data->reader.store(new FileReader(
				prefix + file_data->file_name, file_data));
			//std::cout << "new filereader for " << file_data->file_name << std::endl;
			idx_t pos;
			bool FALSE = false;
			do
			{
				pos = size.load();
				while (!size.compare_exchange_weak(pos, (pos + 1) % max_size));
			} while (!cache_deleting[pos].compare_exchange_weak(FALSE, true));
			if (cache[pos] != NULL /*&& cache[pos]->reader != NULL*/)
			{
				std::printf("remove reader for %s pointer at %d\n",
					cache[pos]->file_name.data(), pos);
				FileReader* d;
				d = cache[pos]->reader.load();
				if (d == NULL || d == (FileReader*)1 || cache[pos]->identify != d->file_data->identify)
					std::cout << "nnononon\n";
				d->file_data->reader_pos = -1;
				while (!d->dec_ref(d->file_data->identify));
				std::printf("done remove reader for %s pointer at %d\n",
					cache[pos]->file_name.data(), pos);
				if (cache[pos]->death)
					delete cache[pos];
				cache_deleting[pos].store(false);
			}
			cache[pos] = file_data;
			file_data->reader_pos = pos;
			std::printf("add reader for %s pointer at %d\n",
				file_data->file_name.data(), file_data->reader_pos);
		}
		while (true)
		{
			if (x == NULL)
				return find(file_data);
			else if (x != (FileReader*)1)
			{
				ref_t ref = util::make_ref(file_data->identify,
					util::unzip_ref_num(x->ref.load()));
				if (x->ref.compare_exchange_weak(ref, ref + 1))
				{
					std::printf("filereader of %s %x ref++,now is %d\n", file_data->file_name.data(),
						x, util::unzip_ref_num(ref + 1));
					//std::printf("get reader for %s pointer is %x\n",
					//	file_data->file_name.data(), x);
					return x;
				}
			}
			x = file_data->reader.load();
		}
	}
	//void FileReaderCache::erase(idx_t pos)
	//{
	//	cache[pos] = NULL;
	//}
//#pragma GCC push_options
//#pragma GCC optimize("O0")
//	inline FileReader* FileReaderCache::wait(FileMetaData*& file_data)
//	{
//		FileReader* x;
//		while (true)
//		{
//			x = file_data->reader.load(std::memory_order_acq_rel);
//			if (x == NULL)
//				find(file_data);
//			else if (x == (FileReader*)1)
//				continue;
//			else /*if (x->ref.load(std::memory_order_acq_rel) != -1
//				&& x->file_data == file_data)*/
//			{
//				//std::printf("get reader for %s pointer is %x\n",
//				//	file_data->file_name.data(), x);
//				ref_t ref = util::make_ref(file_data->identify,
//					util::unzip_ref_num(x->ref.load(std::memory_order_acq_rel)));
//				if (x->ref.compare_exchange_weak(ref, ref + 1,
//					std::memory_order_acq_rel))
//					return x;
//			}
//		}
//	}
	//inline void FileReaderCache::remove(idx_t pos)
	//{
	//	FileReader* d;
	//	do
	//	{
	//		d = cache[pos]->reader.load();
	//	} while (d == NULL || d == (FileReader*)1 || cache[pos]->identify != d->file_data->identify);
	//	d->file_data->reader_pos = -1;
	//	while (!d->dec_ref(d->file_data->identify));
	//	//remove(pos);
	//	if (cache[pos]->death)
	//		delete cache[pos];
	//}
//#pragma GCC pop_options
}
