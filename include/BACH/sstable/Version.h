#pragma once

#include <algorithm>
#include <list>
#include <vector>
#include "BACH/sstable/Compaction.h"

namespace BACH
{
	struct VersionEdit
	{
		std::vector<FileMetaData> EditFileList;
	};
	class Version
	{
	public:
		Version(std::shared_ptr<Options> _option) :
			prev(NULL), next(NULL), epoch(0), option(_option) {}
		Version(Version* _prev, VersionEdit* edit, time_t time) :
			prev(_prev), next(NULL), epoch(std::max(_prev->epoch, time)),
			option(_prev->option)
		{
			prev->next = this;
			FileIndex = prev->FileIndex;
			for (auto& i : edit->EditFileList)
			{
				if (i->deletion)
				{
					auto x = std::lower_bound(FileIndex[i->label][i->level].begin(),
						FileIndex[i->label][i->level].end(), i);
					if ((*x)->file_id != i->file_id ||
						(*x)->vertex_id_b != i->vertex_id_b)
					{
						//error
						exit(-1);
					}
					FileIndex[i->label][i->level].erase(x);
				}
				else
				{
					auto x = std::upper_bound(FileIndex[i->label][i->level].begin(),
						FileIndex[i->label][i->level].end(), i);
					FileIndex[i->label][i->level].insert(x, i);
				}
			}
			for (auto& i : FileIndex)
				for (auto& j : i)
					for (auto& k : j)
						++k->ref;
		}
		~Version()
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
			if (prev != NULL)
				prev->next = next;
			if (next != NULL)
				next->prev = prev;
		}

		Compaction* GetCompaction(VersionEdit* edit)
		{
			label_t label=-1;
			idx_t level = -1;
			vertex_t src_b = -1;
			for (auto i : edit->EditFileList)
				if (!i->deletion)
				{
					label = i->label;
					level = i->level;
					src_b = i->vertex_id_b;
					break;
				}
			vertex_t num = util::ClacFileSize(
				option->MERGE_NUM, level) * option->MERGE_NUM;
			vertex_t tmp = src_b / num;
			auto iter1 = std::lower_bound(FileIndex[label][level].begin(),
				FileIndex[label][level].end(),
				std::make_pair(tmp * num, (idx_t)0));
			auto iter2 = std::lower_bound(FileIndex[label][level].begin(),
				FileIndex[label][level].end(),
				std::make_pair((tmp + 1) * num, (idx_t)0));
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
		void AddRef() { ++ref; }
		void DecRef()
		{
			--ref;
			if (ref == 0)
				delete this;
		}

		std::vector<          //label
			std::vector<      //level
			std::vector<
			FileMetaData*>>> FileIndex;
		Version* prev;
		Version* next;
		time_t epoch;
	private:
		idx_t ref = 1;
		std::shared_ptr<Options> option;
	};
	class VersionIterator
	{
	public:
		VersionIterator(Version* _version, label_t _label, vertex_t _src)
			: version(_version), label(_label), src(_src)
		{
			if (!findsrc())
				nextlevel();
		}
		~VersionIterator() = default;
		FileMetaData* GetFile() const
		{
			if (end)
				return NULL;
			return version->FileIndex[label][level][idx];
		}
		bool End() const { return end; }
		void next()
		{
			if (end)
				return;
			--idx;
			if (version->FileIndex[label][level][idx]->vertex_id_b != src)
				nextlevel();
		}

	private:
		Version* version;
		label_t label;
		vertex_t src;
		idx_t level = 0;
		idx_t idx;
		bool end = false;
		void nextlevel()
		{
			++level;
			while (level < version->FileIndex[label].size() && !findsrc())
				++level;
			if (level == version->FileIndex[label].size())
				end = true;
		}
		bool findsrc()
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
			idx = std::lower_bound(version->FileIndex[label][level].begin(),
				version->FileIndex[label][level].end(),
				std::make_pair(src, NONEINDEX))
				- version->FileIndex[label][level].begin();
			if (version->FileIndex[label][level][idx]->vertex_id_b != src)
				return false;
			return true;
		}
	};

}