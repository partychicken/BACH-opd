#include "BACH/sstable/Version.h"
#include "BACH/db/DB.h"

namespace BACH
{
	Version::Version(DB* _db) :
		prev(NULL), next(NULL), epoch(0), db(_db) {}
	Version::Version(Version* _prev, VersionEdit* edit, time_t time) :
		prev(_prev), next(NULL), epoch(std::max(_prev->epoch, time)),
		db(_prev->db)
	{
		version_name = _prev->version_name + 1;
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
					k->ref.fetch_add(1, std::memory_order_relaxed);
	}
	Version::~Version()
	{
		if (size_entry != NULL)
			size_entry->delete_entry();
		for (auto& i : FileIndex)
			for (auto& j : i)
				for (auto& k : j)
				{
					k->ref.fetch_add(-1, std::memory_order_relaxed);
					if (k->ref.load() == 0)
					{
						unlink((db->options->STORAGE_DIR + "/"
							+ k->file_name).c_str());
						if (k->reader_pos != (idx_t)-1)
							k->death = true;
						else
							delete k;
					}
				}
		if (next != NULL)
		{
			//next->prev = prev;
			next->DecRef();
		}
	}

	Compaction* Version::GetCompaction(VersionEdit* edit)
	{
		label_t label = edit->EditFileList.begin()->label;
		idx_t level = edit->EditFileList.begin()->level;
		vertex_t src_b = edit->EditFileList.begin()->vertex_id_b;
		vertex_t num = util::ClacFileSize(
			db->options, level) * db->options->FILE_MERGE_NUM;
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
		if (cnt < db->options->MEM_TABLE_MAX_SIZE
			* util::fast_pow(db->options->FILE_MERGE_NUM, level + 1)
			* util::fast_pow(10, level))
			return NULL;
		Compaction* c = new Compaction();
		c->vertex_id_b = tmp * num;
		c->vertex_id_e = (tmp + 1) * num - 1;
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
	void Version::AddRef()
	{
		ref.fetch_add(1, std::memory_order_relaxed);
	}
	void Version::DecRef()
	{
		ref.fetch_add(-1, std::memory_order_relaxed);
		bool FALSE = false;
		if (ref.load() == 0 && deleting.compare_exchange_weak(FALSE, true, std::memory_order_relaxed))
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