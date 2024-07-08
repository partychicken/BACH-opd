#pragma once

#include <algorithm>
#include <list>
#include <vector>
#include "BACH/sstable/FileMetaData.h"

namespace BACH
{
	struct VersionEdit
	{
		std::vector<FileMetaData*> EditFileList;
	};
	class Version
	{
	public:
		Version(Version* _prev, VersionEdit* edit) :
			prev(_prev), option(_prev->option)
		{
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
		}

		idx_t GetFileID(label_t label, idx_t level, vertex_t src_b)
		{
			//binary search in FileIndex[label][level]
			idx_t l = 0, r = FileIndex[label][level].size();

			while (l < r)
			{
				idx_t mid = l + r >> 1;
				if (FileIndex[label][level][mid]->vertex_id_b == src_b ?
					FileIndex[label][level][mid]->file_id < NONEINDEX :
					FileIndex[label][level][mid]->vertex_id_b < src_b)
					l = mid + 1;
				else
					r = mid;
			}
			return l;

		}

		std::vector<          //label
			std::vector<      //level
			std::vector<
			FileMetaData*>>> FileIndex;
		Version* prev;
		Version* next;
		time_t epoch;
	private:
		idx_t ref = 0;
		Options* option;
	};

}