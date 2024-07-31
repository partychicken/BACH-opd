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
		Version(std::shared_ptr<Options> _option);
		Version(Version* _prev, VersionEdit* edit, time_t time);
		~Version();

		Compaction* GetCompaction(VersionEdit* edit);
		void AddRef();
		void DecRef();
		void AddSizeEntry(SizeEntry* x);

		std::vector<          //label
			std::vector<      //level
			std::vector<
			FileMetaData*>>> FileIndex;
		Version* prev;
		Version* next;
		time_t epoch;
	private:
		std::atomic<idx_t> ref = 2;
		std::atomic<bool> deleting = false;
		SizeEntry* size_entry = NULL;
		std::shared_ptr<Options> option;
		friend class VersionIterator;
	};
	class VersionIterator
	{
	public:
		VersionIterator(Version* _version, label_t _label, vertex_t _src);
		~VersionIterator() = default;
		FileMetaData* GetFile() const;
		bool End() const { return end; }
		void next();
	private:
		Version* version;
		label_t label;
		vertex_t src;
		idx_t level = -1;
		idx_t idx = 0;
		bool end = false;
		void nextlevel();
		bool findsrc();
	};
	bool FileCompareWithPair(FileMetaData* lhs, std::pair<vertex_t, idx_t> rhs);
	bool FileCompare(FileMetaData* lhs, FileMetaData* rhs);
}