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

		std::vector<          //label
			std::vector<      //level
			std::vector<
			FileMetaData*>>> FileIndex;
	private:
		idx_t ref = 0;
		Version* prev;
		Version* next;
		Options* option;
	};

}