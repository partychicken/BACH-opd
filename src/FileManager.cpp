#include "BACH/sstable/FileManager.h"

namespace BACH
{
	void FileManager::AddCompaction(Compaction& compaction)
	{
		std::unique_lock<std::mutex> lock(CompactionCVMutex);
		CompactionList.push(compaction);
		CompactionCV.notify_one();
	}
}