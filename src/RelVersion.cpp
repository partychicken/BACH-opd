#include "BACH/sstable/RelVersion.h"
#include "BACH/db/DB.h"

namespace BACH
{
	RelVersion::RelVersion(DB* _db) :
		next(nullptr), epoch(0), next_epoch(-1), ref(2), db(_db) {
	}
	RelVersion::RelVersion(RelVersion* _prev, VersionEdit* edit, time_t time) :
		next(nullptr), epoch(std::max(_prev->epoch, time)), next_epoch(-1),
		db(_prev->db)
	{
		FileIndex = _prev->FileIndex;
		FileTotalSize = _prev->FileTotalSize;
		for (auto i : edit->EditFileList)
		{
			if (i->deletion)
			{
				auto x = std::lower_bound(FileIndex[i->level].begin(),
					FileIndex[i->level].end(), i, RelFileCompare<std::string>);
				if ((*x)->file_id != i->file_id)
				{
					//error
					std::cout<<"delete a file that not exist"<<std::endl;
					exit(-1);
				}
				FileIndex[i->level].erase(x);
				FileTotalSize[i->level] -= i->file_size;
			}
			else
			{
				if (FileIndex.size() <= i->level)
					FileIndex.resize(i->level + 1),
					FileTotalSize.resize(i->level + 1);
				auto x = std::upper_bound(FileIndex[i->level].begin(),
					FileIndex[i->level].end(), i, RelFileCompare<std::string>);
				auto f = new RelFileMetaData(*static_cast<RelFileMetaData<std::string> *>(i));
				FileIndex[i->level].insert(x, f);
				FileTotalSize[i->level] += i->file_size;
			}
		}
		for (auto& i : FileIndex)
			for (auto& j : i)
					j->ref.fetch_add(1, std::memory_order_relaxed);
		_prev->next = this;
		_prev->next_epoch = epoch;
	}

	RelVersion::~RelVersion()
	{
		if (size_entry != nullptr)
			size_entry->delete_entry();
		for (auto& i : FileIndex)
			for (auto& j : i)
			{
				auto r = j->ref.fetch_add(-1);
				if (r == 1)
				{
					unlink((db->options->STORAGE_DIR + "/"
						+ j->file_name).c_str());
					//if(k->filter->size() == util::ClacFileSize(db->options, k->level))
					delete j->filter;
					delete j->reader;
					db->ReaderCaches->deletecache(j);
					delete j;
				}
			}
	}

    template<typename Key_t>
	RelCompaction<Key_t>* RelVersion::GetCompaction(VersionEdit* edit, bool force_level)
	{
      	RelFileMetaData<Key_t> *begin_file_meta = *static_cast<RelFileMetaData<Key_t> *>(edit->EditFileList.begin());
      	idx_t level = begin_file_meta->level + 1;
		Key_t key_min = begin_file_meta->key_min;
        Key_t key_max = begin_file_meta->key_max;
		RelCompaction<Key_t>* c = NULL;
		// if (force_level)
		// {
		// 	c = new Compaction();
		// 	c->vertex_id_b = src_b;
		// 	c->label_id = label;
		// 	c->target_level = level;
		// 	auto iter = std::lower_bound(FileIndex[label][level].begin(),
		// 		FileIndex[label][level].end(),
		// 		std::make_pair(src_b + 1, (idx_t)0),
		// 		FileCompareWithPair);
		// 	if (iter == FileIndex[label][level].end() || (*iter)->vertex_id_b != src_b)
		// 		return NULL;
		// 	for (; iter != FileIndex[label][level].end() && (*iter)->vertex_id_b == src_b; --iter)
		// 		if (!(*iter)->merging)
		// 		{
		// 			c->file_list.push_back(*iter);
		// 			(*iter)->merging = true;
		// 			if (iter == FileIndex[label][level].begin())
		// 				break;
		// 		}
		// 		else
		// 		{
		// 			break;
		// 		}
		// 	if (c->file_list.size() <= 1)
		// 	{
		// 		for (auto i : c->file_list)
		// 			i->merging = false;
		// 		delete c;
		// 		c = NULL;
		// 	}
		// }
		if (level == db->options->MAX_LEVEL - 1)
		{
			return c;
		}
//		vertex_t num = util::ClacFileSize(
//			db->options, level) * db->options->FILE_MERGE_NUM;
        //find the last file, whose key_min < current key_min
		auto iter1 = std::lower_bound(FileIndex[level].begin(),
			FileIndex[level].end(),
			std::make_pair(key_min, (idx_t)0),
			FileCompareWithPair);
        if(iter1 != FileIndex[level].begin()) iter1--;

        //find the first file, whose key_min > current key_max
        //thus, the file previous is the last one to compact
		auto iter2 = std::upper_bound(FileIndex[level].begin(),
			FileIndex[level].end(),
			std::make_pair(key_max, (idx_t)0),
			FileCompareWithPair);
		//todo not complement yet
		//size_t cnt = 0;
//		for (auto i = iter1; i != iter2; ++i)
//			if (!(*i)->merging)
//				cnt += (*i)->file_size;
//		if (cnt < db->options->MEM_TABLE_MAX_SIZE
//			* util::fast_pow(db->options->FILE_MERGE_NUM, level + 1))
//		{
//			if (FileTotalSize[level] < db->options->LEVEL_0_MAX_SIZE
//				* util::fast_pow(db->options->LEVEL_SIZE_RITIO, level))
//				return c;
//			bool flag;
//			do
//			{
//				flag = true;
//				vertex_t index = rand() % FileIndex[label][level].size();
//				src_b = FileIndex[label][level][index]->vertex_id_b;
//				tmp = src_b / num;
//				iter1 = std::lower_bound(FileIndex[label][level].begin(),
//					FileIndex[label][level].end(),
//					std::make_pair(tmp * num, (idx_t)0),
//					FileCompareWithPair);
//				iter2 = std::lower_bound(FileIndex[label][level].begin(),
//					FileIndex[label][level].end(),
//					std::make_pair((tmp + 1) * num, (idx_t)0),
//					FileCompareWithPair);
//				for (auto i = iter1; i != iter2; ++i)
//					if (!(*i)->merging)
//					{
//						flag = false;
//						break;
//					}
//			} while (flag);
//		}
		if (c == nullptr)
			c = new Compaction();
		c->key_min = min(key_min, (*iter1)->key_min);
		c->target_level = level;
		for (auto i = iter1; i != iter2; ++i)
			if (!(*i)->merging)
			{
				c->file_list.push_back(*i);
				(*i)->merging = true;
			}
		return c;
	}

	bool RelVersion::AddRef()
	{
		idx_t k;
		do
		{
			k = ref.load();
			if(k == 0)
				return false;
		} while (!ref.compare_exchange_weak(k, k + 1));
		return true;
	}
	void RelVersion::DecRef()
	{
		auto k = ref.fetch_add(-1);
		if (k == 1)
			delete this;
	}
	void RelVersion::AddSizeEntry(std::shared_ptr <relMemTable> x)
	{
		size_entry = x;
	}

    template<typename Key_t>
	RelVersionIterator<Key_t>::RelVersionIterator(RelVersion* _version, Key_t _key_min, Key_t _key_max)
		: version(_version), key_min(_key_min), key_max(_key_max), file_size(1),
		size(version->db->options->MEMORY_MERGE_NUM)
	{
		nextlevel();
	}

    template<typename Key_t>
	FileMetaData* RelVersionIterator<Key_t>::GetFile() const
	{
		if (end)
			return nullptr;
		return version->FileIndex[level][idx];
	}

    template<typename Key_t>
	void RelVersionIterator<Key_t>::next()
	{
		if (end)
			return;
		if (idx == version->FileIndex[level].size() ||
			static_cast<RelFileMetaData<Key_t>* >(version->FileIndex[level][++idx])->key_min > key_max)
			nextlevel();
	}

    template<typename Key_t>
	void RelVersionIterator<Key_t>::nextlevel()
	{
		++level;
		while (level < version->FileIndex.size() && !findsrc())
		{
			++level;
		}
		if (level == version->FileIndex.size())
			end = true;
	}

    template<typename Key_t>
	bool RelVersionIterator<Key_t>::findsrc()
	{
		if (version->FileIndex[level].empty())
			return false;
		auto x = std::lower_bound(version->FileIndex[level].begin(),
			version->FileIndex[level].end(),
			std::make_pair(key_min, 0),
			FileCompareWithPair);
		if (x == version->FileIndex[level].begin())
			return false;
		idx = x - version->FileIndex[level].begin() - 1;
		if (static_cast<RelFileMetaData<Key_t>*>(version->FileIndex[level][idx])->key_min > key_max
			|| static_cast<RelFileMetaData<Key_t>*>(version->FileIndex[level][idx])->key_max < key_min )
			return false;
		return true;
	}

    template<typename Key_t>
	bool RelFileCompareWithPair(RelFileMetaData<Key_t>* lhs, std::pair<Key_t, idx_t> rhs)
	{
		return lhs->key_min == rhs.first ?
			lhs->file_id < rhs.second : lhs->key_min < rhs.first;
	}

    template<typename Key_t>
	bool RelFileCompare(RelFileMetaData<Key_t>* lhs, RelFileMetaData<Key_t>* rhs)
	{
         return lhs->key_min < rhs->key_min;
    }
}