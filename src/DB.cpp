#include "BACH/db/DB.h"
#include "BACH/db/Transaction.h"

namespace BACH
{
	DB::DB(std::shared_ptr<Options> _options) :
		options(_options),
		epoch_id(1),
		write_epoch_table(_options->MAX_WORKER_THREAD)
	{
		if(options->MAX_FILE_READER_CACHE_SIZE != 0)
		{
			ReaderCaches = std::make_unique<FileReaderCache>(
				options->MAX_FILE_READER_CACHE_SIZE, options->STORAGE_DIR + "/");
		}
		else
		{
			struct rlimit rlim;
			if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
				perror("getrlimit failed");
				exit(-1);
			}
			rlim.rlim_cur = rlim.rlim_max;
			if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
				perror("setrlimit failed");
				exit(-1);
			}
			ReaderCaches = std::make_unique<FileReaderCache>(0, options->STORAGE_DIR + "/");
		}
		Labels = std::make_unique<LabelManager>();
		Memtable = std::make_unique<MemoryManager>(this);
		Files = std::make_unique<FileManager>(this);
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
		close = true;
		Files->CompactionCV.notify_all();
		for(auto &i: compact_thread)
			if (i->joinable())
				i->join();
		read_version.load()->DecRef();
		current_version->DecRef();
		//std::cout << "closed" << std::endl;
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
		return Transaction(local_write_epoch_id, local_read_epoch_id, this,
			get_read_version(), pos);
	}
	Transaction DB::BeginReadOnlyTransaction()
	{
		time_t local_epoch_id = get_read_time();
		//std::shared_lock<std::shared_mutex>versionlock(version_mutex);
		return Transaction(MAXTIME, local_epoch_id, this, get_read_version());
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
	void DB::CompactAll(double_t ratio)
	{
		if(options->MERGING_STRATEGY != Options::MergingStrategy::LEVELING)
			return;
		//Memtable->PersistenceAll();
		while (working_compact_thread.load() > 0 || !Files->CompactionList.empty())
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		for (idx_t level = 0; level < options->MAX_LEVEL - 1; level++)
		{
			auto v = current_version;
			for(label_t label = 0; label < v->FileIndex.size(); label++)
			{
				if(v->FileIndex[label].size() <= level)
					continue;
				if(v->FileIndex[label][level].size() == 1)
					continue;
				auto &i = v->FileIndex[label];
				vertex_t last = 0;
				auto &j = i[level];
				if(j.size() == 0)
					continue;
				size_t idx = 1;
				vertex_t bound = std::round(ratio * j.size());
				if(bound == 0)
					continue;
				for(; idx < j.size() && idx < bound; idx++)
					if((j[idx]->vertex_id_b - j[last]->vertex_id_b)
						/ util::ClacFileSize(options, level + 1) != 0)
					{
						Compaction x;
						x.file_id = -1;
						x.label_id = j[last]->label;
						x.target_level = j[last]->level + 1;
						x.vertex_id_b = j[last]->vertex_id_b;
						for(size_t k = last; k < idx; k++)
						{
							j[k]->merging = true;
							x.file_list.push_back(j[k]);
						}
						Files->CompactionList.push(x);
						last = idx;
					}
				for(; idx < j.size() - 1 && j[idx]->vertex_id_b == j[idx + 1]->vertex_id_b; idx++);
				if(idx - last > 1)
				{
					Compaction x;
					x.file_id = -1;
					x.label_id = j[last]->label;
					x.target_level = j[last]->level + 1;
					x.vertex_id_b = j[last]->vertex_id_b;
					for(size_t k = last; k < idx; k++)
					{
						j[k]->merging = true;
						x.file_list.push_back(j[k]);
					}
					Files->CompactionList.push(x);
				}
			}
			Files->CompactionCV.notify_all();
			while (working_compact_thread.load() > 0 || !Files->CompactionList.empty())
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		}
		auto v = current_version;
		for(auto &i: v->FileIndex)
			for(auto &j: i)
				if(j.size() > 0)
				{
					vertex_t last = 0;
					for(size_t idx = 1; idx < j.size(); idx++)
						if(j[idx]->vertex_id_b != j[last]->vertex_id_b)
						{
							if(idx - last > 1)
							{
								Compaction x;
								x.file_id = -1;
								x.label_id = j[last]->label;
								x.target_level = j[last]->level;
								x.vertex_id_b = j[last]->vertex_id_b;
								for(size_t k = last; k < idx; k++)
								{
									j[k]->merging = true;
									x.file_list.push_back(j[k]);
								}
								Files->CompactionList.push(x);
							}
							last = idx;
						}
					if(j.size() - last > 1)
					{
						Compaction x;
						x.file_id = -1;
						x.label_id = j[last]->label;
						x.target_level = j[last]->level;
						x.vertex_id_b = j[last]->vertex_id_b;
						for(size_t k = last; k < j.size(); k++)
						{
							j[k]->merging = true;
							x.file_list.push_back(j[k]);
						}
						Files->CompactionList.push(x);
					}
				}
		Files->CompactionCV.notify_all();
		while (working_compact_thread.load() > 0 || !Files->CompactionList.empty())
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
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
				working_compact_thread.fetch_add(1, std::memory_order_relaxed);
				Compaction x(Files->CompactionList.front());
				Files->CompactionList.pop();
				lock.unlock();
				VersionEdit* edit;
				time_t time = 0;
				x.file_id = Files->GetFileID(
					x.label_id, x.target_level, x.vertex_id_b);
				idx_t type = 2;
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
						std::unique_lock<std::mutex> versionlock(version_mutex);
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
						if ((*iter)->vertex_id_b == x.vertex_id_b)
						{
							if ((*iter)->merging == false)
							{
								(*iter)->merging = true;
								x.file_list.push_back(*iter);
							}
						}
						break;
					}
					edit = Files->MergeSSTable(x);
				}
				ProgressVersion(edit, time, x.Persistence, type == 1);
				delete edit;
				working_compact_thread.fetch_add(-1, std::memory_order_relaxed);
				ProgressReadVersion();
			}
			else
			{
				Files->CompactionCV.wait_for(lock, std::chrono::milliseconds(200));
				//ProgressReadVersion();
			}
		}
	}
	void DB::ProgressVersion(VersionEdit* edit, time_t time,
		std::shared_ptr<SizeEntry> size, bool force_leveling)
	{
		std::unique_lock<std::mutex> version_lock(version_mutex);
		Version* tmp = current_version;
		tmp->AddSizeEntry(size);
		current_version = new Version(tmp, edit, time);
		auto compact = current_version->GetCompaction(edit, force_leveling);
		if (compact != NULL)
		{
			Files->AddCompaction(*compact);
			delete compact;
		}
		version_lock.unlock();
	}
	void DB::ProgressReadVersion()
	{
		while (true)
		{
			time_t nrt = get_read_time();
			Version* tail = read_version.load();
			if (tail->next_epoch == (time_t)-1 || tail->next_epoch >= nrt)
				break;
			if (read_version.compare_exchange_weak(tail, tail->next))
			{
				tail->DecRef();
			}
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
	Version* DB::get_read_version()
	{
		Version* version;
		while(true)
		{
			version = read_version.load();
			if(version->AddRef())
				return version;
		}
	}
}