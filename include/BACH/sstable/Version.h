#pragma once

#include <algorithm>
#include <list>
#include <vector>
#include "BACH/sstable/Compaction.h"

namespace BACH
{
	class DB;
	struct VersionEdit
	{
		std::vector<FileMetaData> EditFileList;
	};
	class Version
	{
	public:
		Version(DB* _db);
		Version(Version* _prev, VersionEdit* edit, time_t time);
		~Version();

		Compaction* GetCompaction(VersionEdit* edit, bool force_level = false);
		bool AddRef();
		void DecRef();
		void AddSizeEntry(std::shared_ptr < SizeEntry > x);

		std::vector<          //label
			std::vector<      //level
			std::vector<
			FileMetaData*>>> FileIndex;
		Version* next;
		time_t epoch;
		time_t next_epoch;

	private:
		std::vector<          //label
			std::vector<      //level
			size_t>> FileTotalSize;
		std::atomic<idx_t> ref = 1;
		std::shared_ptr < SizeEntry > size_entry = NULL;
		DB* db;
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
		vertex_t file_size, size;
		bool end = false;
		void nextlevel();
		bool findsrc();
	};
	bool FileCompareWithPair(FileMetaData* lhs, std::pair<vertex_t, idx_t> rhs);
	bool FileCompare(FileMetaData* lhs, FileMetaData* rhs);
}