#include "BACH/sstable/Version.h"

namespace BACH
{
	Version::Version(std::shared_ptr<Options> _option) :
		prev(NULL), next(NULL), epoch(0), option(_option) {}
	Version::Version(Version* _prev, VersionEdit* edit, time_t time) :
		prev(_prev), next(NULL), epoch(std::max(_prev->epoch, time)),
		option(_prev->option)
	{
		version_name = _prev->version_name + 1;
		std::cout << "version " << version_name << " created!" << std::endl;
		prev->next = this;
		FileIndex = prev->FileIndex;
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
					exit(-1);
				}
				FileIndex[i.label][i.level].erase(x);
			}
			else
			{
				while (FileIndex.size() <= i.label)
					FileIndex.emplace_back();
				while (FileIndex[i.label].size() <= i.level)
					FileIndex[i.label].emplace_back();
				auto x = std::upper_bound(FileIndex[i.label][i.level].begin(),
					FileIndex[i.label][i.level].end(), &i, FileCompare);
				auto f = new FileMetaData(i);
				FileIndex[i.label][i.level].insert(x, f);
			}
		}
		for (auto& i : FileIndex)
			for (auto& j : i)
				for (auto& k : j)
					++k->ref;
	}
	Version::~Version()
	{
		for (auto& i : FileIndex)
			for (auto& j : i)
				for (auto& k : j)
				{
					--k->ref;
					if (k->ref == 0)
					{
						unlink((option->STORAGE_DIR + "/"
							+ k->file_name).c_str());
					}
				}
		std::cout << "version " << version_name << " deleted!" << std::endl;
		if (prev != NULL)
			prev->next = next;
		if (next != NULL)
			next->prev = prev;
	}

	Compaction* Version::GetCompaction(VersionEdit* edit)
	{
		label_t label = -1;
		idx_t level = -1;
		vertex_t src_b = -1;
		for (auto &i : edit->EditFileList)
			if (!i.deletion)
			{
				label = i.label;
				level = i.level;
				src_b = i.vertex_id_b;
				break;
			}
		vertex_t num = util::ClacFileSize(
			option->MERGE_NUM, level) * option->MERGE_NUM;
		vertex_t tmp = src_b / num;
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
		if (cnt < option->MEM_TABLE_MAX_SIZE * util::ClacFileSize(
			option->MERGE_NUM, level))
			return NULL;
		Compaction* c = new Compaction();
		c->vertex_id_b = tmp * num;
		c->vertex_id_e = (tmp + 1) * num - 1;
		c->label_id = label;
		c->target_level = level + 1;
		for (auto i = iter1; i != iter2; ++i)
		{
			c->file_list.push_back(**i);
			(*i)->merging = true;
		}
		return c;
	}
	void Version::AddRef()
	{
		++ref;
		std::cout << "version " << version_name << " ref++, now is " << ref << std::endl;
	}
	void Version::DecRef()
	{
		--ref;
		std::cout << "version " << version_name << " ref--, now is " << ref << std::endl;
		bool FALSE = false;
		if (ref == 0 && deleting.compare_exchange_weak(FALSE, true, std::memory_order_relaxed))
			delete this;
	}

	VersionIterator::VersionIterator(Version* _version, label_t _label, vertex_t _src)
		: version(_version), label(_label), src(_src)
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
		--idx;
		if (version->FileIndex[label][level][idx]->vertex_id_b != src)
			nextlevel();
	}

	void VersionIterator::nextlevel()
	{
		while (level < version->FileIndex[label].size() && !findsrc())
			++level;
		if (level == version->FileIndex[label].size())
			end = true;
	}
	bool VersionIterator::findsrc()
	{
		if (version->FileIndex[label][level].empty())
			return false;
		//idx_t l = 0, r = version->FileIndex[label][level].size();
		//while (l < r)
		//{
		//	idx_t mid = l + r >> 1;
		//	if (version->FileIndex[label][level][mid]->vertex_id_b == src ?
		//		version->FileIndex[label][level][mid]->file_id < NONEINDEX :
		//		version->FileIndex[label][level][mid]->vertex_id_b < src)
		//		l = mid + 1;
		//	else
		//		r = mid;
		//}
		//if (version->FileIndex[label][level][l]->vertex_id_b != src)
		//	return false;
		auto x = std::lower_bound(version->FileIndex[label][level].begin(),
			version->FileIndex[label][level].end(),
			std::make_pair(src, NONEINDEX),
			FileCompareWithPair);
		if (x == version->FileIndex[label][level].end())
			return false;
		idx = x - version->FileIndex[label][level].begin();
		if (version->FileIndex[label][level][idx]->vertex_id_b != src)
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