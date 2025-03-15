#include "BACH/sstable/Version.h"
#include "BACH/db/DB.h"

namespace BACH
{
	Version::Version(DB* _db) :
		next(NULL), epoch(0), next_epoch(-1), ref(2), db(_db) {
	}
	Version::Version(Version* _prev, VersionEdit* edit, time_t time) :
		next(NULL), epoch(std::max(_prev->epoch, time)), next_epoch(-1),
		db(_prev->db)
	{
		FileIndex = _prev->FileIndex;
		FileTotalSize = _prev->FileTotalSize;
		for (auto& i : edit->EditFileList)
		{
			if (i.deletion)
			{
				auto x = std::lower_bound(FileIndex[i.label][i.level].begin(),
					FileIndex[i.label][i.level].end(), &i, FileCompare);
				if ((*x)->file_id != i.file_id ||
					(*x)->vertex_id_b != i.vertex_id_b)
				{
					//error
					std::cout<<"delete a file that not exist"<<std::endl;
					exit(-1);
				}
				FileIndex[i.label][i.level].erase(x);
				FileTotalSize[i.label][i.level] -= i.file_size;
			}
			else
			{
				if (FileIndex.size() <= i.label)
					FileIndex.resize(i.label + 1),
					FileTotalSize.resize(i.label + 1);
				if (FileIndex[i.label].size() <= i.level)
					FileIndex[i.label].resize(i.level + 1),
					FileTotalSize[i.label].resize(i.level + 1);
				auto x = std::upper_bound(FileIndex[i.label][i.level].begin(),
					FileIndex[i.label][i.level].end(), &i, FileCompare);
				auto f = new FileMetaData(std::move(i));
				FileIndex[i.label][i.level].insert(x, f);
				FileTotalSize[i.label][i.level] += i.file_size;
			}
		}
		for (auto& i : FileIndex)
			for (auto& j : i)
				for (auto& k : j)
					k->ref.fetch_add(1, std::memory_order_relaxed);
		_prev->next = this;
		_prev->next_epoch = epoch;
	}
	Version::~Version()
	{
		if (size_entry != NULL)
			size_entry->delete_entry();
		for (auto& i : FileIndex)
			for (auto& j : i)
				for (auto& k : j)
				{
					auto r = k->ref.fetch_add(-1);
					if (r == 1)
					{
						unlink((db->options->STORAGE_DIR + "/"
							+ k->file_name).c_str());
						//if(k->filter->size() == util::ClacFileSize(db->options, k->level))
						delete k->filter;
						if (k->reader != NULL)
							delete k->reader;
						db->ReaderCaches->deletecache(k);
						delete k;
					}
				}
	}

	Compaction* Version::GetCompaction(VersionEdit* edit, bool force_level)
	{
		label_t label = edit->EditFileList.begin()->label;
		idx_t level = edit->EditFileList.begin()->level;
		vertex_t src_b = edit->EditFileList.begin()->vertex_id_b;
		Compaction* c = NULL;
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
		vertex_t num = util::ClacFileSize(
			db->options, level) * db->options->FILE_MERGE_NUM;
		vertex_t tmp = (num == 0) ? 0 : src_b / num;
		auto iter1 = std::lower_bound(FileIndex[label][level].begin(),
			FileIndex[label][level].end(),
			std::make_pair(tmp * num, (idx_t)0),
			FileCompareWithPair);
		auto iter2 = std::lower_bound(FileIndex[label][level].begin(),
			FileIndex[label][level].end(),
			std::make_pair((tmp + 1) * num, (idx_t)0),
			FileCompareWithPair);
		size_t cnt = 0;
		for (auto i = iter1; i != iter2; ++i)
			if (!(*i)->merging)
				cnt += (*i)->file_size;
		if (cnt < db->options->MEM_TABLE_MAX_SIZE
			* util::fast_pow(db->options->FILE_MERGE_NUM, level + 1))
		{
			if (FileTotalSize[label][level] < db->options->LEVEL_0_MAX_SIZE
				* util::fast_pow(db->options->LEVEL_SIZE_RITIO, level))
				return c;
			bool flag;
			do
			{
				flag = true;
				vertex_t index = rand() % FileIndex[label][level].size();
				src_b = FileIndex[label][level][index]->vertex_id_b;
				tmp = src_b / num;
				iter1 = std::lower_bound(FileIndex[label][level].begin(),
					FileIndex[label][level].end(),
					std::make_pair(tmp * num, (idx_t)0),
					FileCompareWithPair);
				iter2 = std::lower_bound(FileIndex[label][level].begin(),
					FileIndex[label][level].end(),
					std::make_pair((tmp + 1) * num, (idx_t)0),
					FileCompareWithPair);
				for (auto i = iter1; i != iter2; ++i)
					if (!(*i)->merging)
					{
						flag = false;
						break;
					}
			} while (flag);
		}
		if (c == NULL)
			c = new Compaction();
		c->vertex_id_b = tmp * num;
		c->label_id = label;
		c->target_level = level + 1;
		for (auto i = iter1; i != iter2; ++i)
			if (!(*i)->merging)
			{
				c->file_list.push_back(*i);
				(*i)->merging = true;
			}
		return c;
	}

	bool Version::AddRef()
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
	void Version::DecRef()
	{
		auto k = ref.fetch_add(-1);
		if (k == 1)
			delete this;
	}
	void Version::AddSizeEntry(std::shared_ptr < SizeEntry > x)
	{
		size_entry = x;
	}

	VersionIterator::VersionIterator(Version* _version, label_t _label, vertex_t _src)
		: version(_version), label(_label), src(_src), file_size(1),
		size(version->db->options->MEMORY_MERGE_NUM)
	{
		if (version->FileIndex.size() <= label)
		{
			end = true;
			return;
		}
		nextlevel();
	}
	FileMetaData* VersionIterator::GetFile() const
	{
		if (end)
			return NULL;
		return version->FileIndex[label][level][idx];
	}
	void VersionIterator::next()
	{
		if (end)
			return;
		if (idx == 0 || version->FileIndex[label][level][--idx]->vertex_id_b != src * file_size)
			nextlevel();
	}

	void VersionIterator::nextlevel()
	{
		++level;
		src = src / size;
		file_size *= size;
		size = version->db->options->FILE_MERGE_NUM;
		while (level < version->FileIndex[label].size() && !findsrc())
		{
			++level;
			src = src / size;
			file_size *= size;
		}
		if (level == version->FileIndex[label].size())
			end = true;
	}
	bool VersionIterator::findsrc()
	{
		if (version->FileIndex[label][level].empty())
			return false;
		auto x = std::lower_bound(version->FileIndex[label][level].begin(),
			version->FileIndex[label][level].end(),
			std::make_pair(src * file_size + 1, 0),
			FileCompareWithPair);
		if (x == version->FileIndex[label][level].begin())
			return false;
		idx = x - version->FileIndex[label][level].begin() - 1;
		if (version->FileIndex[label][level][idx]->vertex_id_b != src * file_size)
			return false;
		return true;
	}
	bool FileCompareWithPair(FileMetaData* lhs, std::pair<vertex_t, idx_t> rhs)
	{
		return lhs->vertex_id_b == rhs.first ?
			lhs->file_id < rhs.second : lhs->vertex_id_b < rhs.first;
	}
	bool FileCompare(FileMetaData* lhs, FileMetaData* rhs)
	{
		return lhs->vertex_id_b == rhs->vertex_id_b ?
			lhs->file_id < rhs->file_id : lhs->vertex_id_b < rhs->vertex_id_b;
	}
}