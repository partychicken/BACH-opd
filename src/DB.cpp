#include "BACH/db/DB.h"

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
	}
	DB::~DB()
	{
		std::cout << "closed" << std::endl;
		close = true;
		Files->CompactionCV.notify_all();
	}
	Transaction DB::BeginTransaction()
	{
		std::unique_lock<std::shared_mutex> lock(epoch_table_mutex);
		time_t local_epoch_id = epoch_id.fetch_add(1, std::memory_order_relaxed);
		epoch_table.insert(local_epoch_id);
		return Transaction(local_epoch_id, this, false);
	}
	Transaction DB::BeginReadOnlyTransaction()
	{
		time_t local_epoch_id;
		std::shared_lock<std::shared_mutex> lock(epoch_table_mutex);
		if (epoch_table.empty())
			local_epoch_id = epoch_id.load(std::memory_order_acquire);
		else
			local_epoch_id = (*epoch_table.begin()) - 1;
		return Transaction(local_epoch_id, this, true);
	}

	label_t DB::AddVertexLabel(std::string_view label_name)
	{
		Memtable->AddVertexLabel();
		return Labels->AddVertexLabel(label_name);
	}
	label_t DB::AddEdgeLabel(std::string_view edge_label_name,
		std::string_view src_label_name, std::string_view dst_label_name)
	{
		auto x = Labels->AddEdgeLabel(
			edge_label_name, src_label_name, dst_label_name);
		Memtable->AddEdgeLabel(std::get<1>(x), std::get<2>(x));
		return std::get<0>(x);
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
				if (x.Persistence != NULL)
				{
					//persistence
					Memtable->MemTablePersistence(
						x.label_id, x.Persistence, epoch_id.load());
				}
				else
				{
					//merge
					//Files->MergeSSTable(x, epoch_id, deadtime);
				}
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
	void DB::CompactAll()
	{
		Memtable->PersistenceAll();
		//bool done;
		//do
		//{
		//	done = true;
		//}while (done);
	}
}