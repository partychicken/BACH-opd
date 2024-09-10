#include "BACH/db/DB.h"
#include "BACH/db/Transaction.h"

namespace BACH
{
	DB::DB(std::shared_ptr<Options> _options) :
		options(_options),
		epoch_id(1),
		write_epoch_table(_options->MAX_WORKER_THREAD)
	{
		Labels = std::make_unique<LabelManager>();
		Memtable = std::make_unique<MemoryManager>(this);
		Files = std::make_unique<FileManager>(this);
		ReaderCaches = std::make_unique<FileReaderCache>(
			options->MAX_FILE_READER_CACHE_SIZE, options->STORAGE_DIR + "/");
		for (idx_t i = 0; i < _options->NUM_OF_COMPACTION_THREAD; ++i)
		{
			compact_thread.push_back(std::make_shared<std::thread>(
				[&] {CompactLoop(); }));
			compact_thread[i]->detach();
		}
		read_version = current_version = new Version(this);
	}
	DB::~DB()
	{
		std::cout << "closed" << std::endl;
		close = true;
		std::unique_lock <std::mutex> lock(Files->CloseCVMutex);
		if (Files->CompactionList.empty())
		{
			Files->CompactionCV.notify_all();
			//break;
		}
		else
		{
			Files->CloseCV.wait(lock);
		}
	}
	Transaction DB::BeginTransaction()
	{
		//std::unique_lock<std::shared_mutex> wlock(write_epoch_table_mutex);
		time_t local_write_epoch_id = epoch_id.fetch_add(1, std::memory_order_relaxed);
		size_t pos = write_epoch_table.insert(local_write_epoch_id);
		time_t local_read_epoch_id;
		local_read_epoch_id = write_epoch_table.find_min() - 1;
		//local_read_epoch_id = (*write_epoch_table.begin()) - 1;
		//wlock.unlock();
		std::shared_lock<std::shared_mutex>versionlock(version_mutex);
		return Transaction(local_write_epoch_id, local_read_epoch_id, this,
			read_version, pos);
	}
	Transaction DB::BeginReadOnlyTransaction()
	{
		time_t local_epoch_id = get_read_time();
		std::shared_lock<std::shared_mutex>versionlock(version_mutex);
		return Transaction(MAXTIME, local_epoch_id, this, read_version);
	}

	label_t DB::AddVertexLabel(std::string label_name)
	{
		Memtable->AddVertexLabel();
		return Labels->AddVertexLabel(label_name);
	}
	label_t DB::AddEdgeLabel(std::string edge_label_name,
		std::string src_label_name, std::string dst_label_name)
	{
		auto x = Labels->AddEdgeLabel(
			edge_label_name, src_label_name, dst_label_name);
		Memtable->AddEdgeLabel(std::get<1>(x), std::get<2>(x));
		return std::get<0>(x);
	}
	void DB::CompactAll()
	{
		//Memtable->PersistenceAll();
		//bool done;
		//do
		//{
		//	done = true;
		//}while (done);
	}
	void DB::CompactLoop()
	{
		while (true)
		{
			if (close)
				return;
			std::unique_lock <std::mutex> lock(Files->CompactionCVMutex);
			if (!Files->CompactionList.empty())
			{
				Compaction x(Files->CompactionList.front());
				Files->CompactionList.pop();
				lock.unlock();
				VersionEdit* edit;
				time_t time = 0;
				x.file_id = Files->GetFileID(
					x.label_id, x.target_level, x.vertex_id_b);
				if (x.Persistence != NULL)
				{
					//persistence
					edit = Memtable->MemTablePersistence(x.label_id, x.file_id,
						x.Persistence);
					time = x.Persistence->max_time;
				}
				else
				{
					//choose merge type
					idx_t type;
					switch (options->MERGING_STRATEGY)
					{
					case Options::MergingStrategy::LEVELING:
						type = 1;
						break;
					case Options::MergingStrategy::TIERING:
						type = 2;
						break;
					case Options::MergingStrategy::ELASTIC:
						type = Memtable->GetMergeType(x.label_id, x.vertex_id_b, x.target_level);
						break;
					default:
						type = 0;
						break;
					}
					switch (type)
					{
					case 2:
						break;
					case 1:
						std::unique_lock<std::shared_mutex> versionlock(version_mutex);
						if (current_version->FileIndex[x.label_id].size() <= x.target_level)
							break;
						auto iter = std::lower_bound(
							current_version->FileIndex[x.label_id][x.target_level].begin(),
							current_version->FileIndex[x.label_id][x.target_level].end(),
							std::make_pair(x.vertex_id_b + 1, 0),
							FileCompareWithPair);
						if (iter == current_version->FileIndex[x.label_id][x.target_level].begin())
							break;
						iter--;
						while ((*iter)->vertex_id_b == x.vertex_id_b)
						{
							if ((*iter)->merging == false)
							{
								(*iter)->merging = true;
								x.file_list.push_back(*iter);
								break;
							}
							if (iter == current_version->FileIndex[x.label_id][x.target_level].begin())
								break;
							iter--;
						}
						break;
					}
					edit = Files->MergeSSTable(x);
				}
				std::unique_lock<std::shared_mutex> version_lock(version_mutex);
				Version* tmp = current_version;
				tmp->AddSizeEntry(x.Persistence);
				current_version = new Version(tmp, edit, time);
				auto compact = current_version->GetCompaction(edit);
				if (compact != NULL)
					Files->AddCompaction(*compact);
				tmp->DecRef();
				version_lock.unlock();
				ProgressReadVersion();
				Files->CloseCV.notify_one();
			}
			else
			{
				Files->CompactionCV.wait(lock);
				if (close)
					return;
			}
		}
	}
	void DB::ProgressReadVersion()
	{
		std::unique_lock<std::shared_mutex>lock(version_mutex);
		while (read_version->next != NULL &&
			read_version->next->epoch < get_read_time())
		{
			Version* tmp = read_version;
			read_version = read_version->next;
			read_version->AddRef();
			tmp->DecRef();
		}
	}
	time_t DB::get_read_time()
	{
		//std::shared_lock<std::shared_mutex> wlock(write_epoch_table_mutex);
		if (write_epoch_table.empty())
			return epoch_id.load(std::memory_order_acquire);
		else
			//return (*write_epoch_table.begin()) - 1;
			return write_epoch_table.find_min() - 1;
	}
}