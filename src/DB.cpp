#include "BACH/db/DB.h"
#include "BACH/db/Transaction.h"

namespace BACH
{
	DB::DB(std::shared_ptr<Options> _options) :
		options(_options),
		epoch_id(0)
	{
		Labels = std::make_unique<LabelManager>();
		Memtable = std::make_unique<MemoryManager>(this);
		Files = std::make_unique<FileManager>(this);
		for (idx_t i = 0; i < _options->NUM_OF_COMPACTION_THREAD; ++i)
		{
			compact_thread.push_back(std::make_shared<std::thread>(
				[&] {CompactLoop(); }));
			compact_thread[i]->detach();
		}
		read_version = current_version = new Version(options);
	}
	DB::~DB()
	{
		std::cout << "closed" << std::endl;
		close = true;
		Files->CompactionCV.notify_all();
	}
	Transaction DB::BeginWriteTransaction()
	{
		std::unique_lock<std::shared_mutex> lock(write_epoch_table_mutex);
		time_t local_epoch_id = epoch_id.fetch_add(1, std::memory_order_relaxed);
		write_epoch_table.insert(local_epoch_id);
		return Transaction(local_epoch_id, this, NULL, false);
	}
	Transaction DB::BeginReadTransaction()
	{
		time_t local_epoch_id;
		std::shared_lock<std::shared_mutex> wlock(write_epoch_table_mutex);
		if (write_epoch_table.empty())
			local_epoch_id = epoch_id.load(std::memory_order_acquire);
		else
			local_epoch_id = (*write_epoch_table.begin()) - 1;
		std::unique_lock<std::shared_mutex> rlock(read_epoch_table_mutex);
		if (read_epoch_table.find(local_epoch_id) == read_epoch_table.end())
			read_epoch_table[local_epoch_id] = 1;
		else
			++read_epoch_table[local_epoch_id];
		return Transaction(local_epoch_id, this, read_version, true);
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
		Memtable->PersistenceAll();
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
			std::unique_lock <std::mutex> lock(Files->CompactionCVMutex);
			if (!Files->CompactionList.empty())
			{
				Compaction x(Files->CompactionList.front());
				Files->CompactionList.pop();
				lock.unlock();
				VersionEdit* edit;
				time_t time = 0;
				if (x.Persistence != NULL)
				{
					//persistence
					edit = Memtable->MemTablePersistence(x.label_id,
						x.Persistence);
					time = x.persistece_time;
				}
				else
				{
					//merge
					//Files->MergeSSTable(x, epoch_id, deadtime);
				}
				std::unique_lock<std::shared_mutex> vlock(version_mutex);
				Version* tmp = current_version;
				current_version = new Version(tmp, edit, time);
				tmp->DecRef();
			}
			else
			{
				Files->CompactionCV.wait(lock);
				if (close)
				{
					return;
				}
			}
		}
	}
	void DB::ProgressReadVersion()
	{
		std::unique_lock<std::shared_mutex>lock(version_mutex);
		while (read_version->next->epoch < get_read_time())
		{
			Version* tmp = read_version;
			read_version = read_version->next;
			read_version->AddRef();
			tmp->DecRef();
		}
	}
	time_t DB::get_read_time()
	{
		std::shared_lock<std::shared_mutex> wlock(write_epoch_table_mutex);
		if (write_epoch_table.empty())
			return epoch_id.load(std::memory_order_acquire);
		else
			return (*write_epoch_table.begin()) - 1;
	}
}