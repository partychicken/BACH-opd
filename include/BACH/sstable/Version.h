#pragma once

#include <algorithm>
#include <list>
#include <vector>
#include "BACH/sstable/FileMetaData.h"

namespace BACH
{
	struct VersionEdit
	{
		std::vector<          //label
			std::vector<      //level
			std::vector<
			FileMetaData*>>> EditFileList;
	};
	class Version
	{
	public:
		Version(Version* _prev, VersionEdit& edit) :
			prev(_prev), option(_prev->option)
		{
			FileIndex = prev->FileIndex;
			for (auto& i : edit.EditFileList)
				for (auto& j : i)
				{
					if (j.empty()) continue;
					//idx_t file_cnt = FileIndex[(*j.begin())->level].size();
					for (auto& k : j)
					{
						if (k->deletion)
						{
							auto x = std::lower_bound(FileIndex[k->label][k->level].begin(),
								FileIndex[k->label][k->level].end(), k);
							*x = FileIndex[k->label][k->level].back();
							FileIndex[k->label][k->level].pop_back();
						}
						else
						{
							FileIndex[k->label][k->level].push_back(k);
						}
					}
					sort(FileIndex[(*j.begin())->label][(*j.begin())->level].begin(),
						FileIndex[(*j.begin())->label][(*j.begin())->level].end());
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