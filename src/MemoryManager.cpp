#include "BACH/memory/MemoryManager.h"
#include "BACH/db/DB.h"

namespace BACH
{
	MemoryManager::MemoryManager(DB* _db) :
		db(_db)
	{

	}

	void MemoryManager::AddVertexLabel()
	{
		VertexLabelIndex.push_back(new VertexLabelEntry());
	}
	void MemoryManager::AddEdgeLabel(label_t src_label, label_t dst_label)
	{
		EdgeLabelIndex.push_back(new EdgeLabelEntry(
			src_label, db->options->QUERY_LIST_SIZE));
	}

	vertex_t MemoryManager::AddVertex(label_t label_id,
		std::string_view property, time_t now_time)
	{
		std::unique_lock<std::shared_mutex> lock(VertexLabelIndex[label_id]->mutex);
		vertex_t vertex_id = VertexLabelIndex[label_id]->total_vertex++;
		VertexLabelIndex[label_id]->VertexProperty.push_back(
			static_cast<std::string>(property));
		for (label_t i = 0; i < EdgeLabelIndex.size(); ++i)
		{
			if (EdgeLabelIndex[i]->src_label_id == label_id)
			{
				std::unique_lock<std::shared_mutex> lock(EdgeLabelIndex[i]->mutex);
				if (EdgeLabelIndex[i]->now_size_info->entry.size() >= db->options->MERGE_NUM)
				{
					EdgeLabelIndex[i]->now_size_info =
						new SizeEntry(vertex_id, NULL);
				}
				EdgeLabelIndex[i]->VertexIndex.push_back(
					new VertexEntry(EdgeLabelIndex[i]->now_size_info));
				EdgeLabelIndex[i]->query_counter.AddVertex();
				EdgeLabelIndex[i]->vertex_mutex.emplace_back_default();
			}
		}
		VertexLabelIndex[label_id]->property_size += property.size();
		if (VertexLabelIndex[label_id]->property_size >
			db->options->VERTEX_PROPERTY_MAX_SIZE)
		{
			vertex_property_persistence(label_id);
		}
		lock.unlock();
		return vertex_id;
	}
	std::shared_ptr<std::string> MemoryManager::GetVertex(vertex_t vertex, label_t label,
		time_t now_time)
	{
		std::shared_lock<std::shared_mutex> lock(VertexLabelIndex[label]->mutex);
		auto x = VertexLabelIndex[label]->deletetime.find(vertex);
		if (x == VertexLabelIndex[label]->deletetime.end()
			|| x->second > now_time)
		{
			if (vertex >= VertexLabelIndex[label]->unpersistence)
			{
				return std::make_shared<std::string>(
					VertexLabelIndex[label]->VertexProperty[vertex]);
			}
			else
			{
				idx_t file_no = VertexLabelIndex[label]->FileIndex.lowerbound(vertex);
				auto reader = std::make_shared<FileReader>(static_cast<std::string>(
					db->Labels->GetVertexLabel(label)) + "_"
					+ std::to_string(file_no) + ".property");
				auto parser = std::make_shared<PropertyFileParser>(
					reader);
				return parser->GetProperty(vertex - file_no);
			}
		}
		else
		{
			return NULL;
		}
	}
	void MemoryManager::DelVertex(vertex_t vertex, label_t label, time_t now_time)
	{
		std::unique_lock<std::shared_mutex> lock(VertexLabelIndex[label]->mutex);
		VertexLabelIndex[label]->deletetime[vertex] = now_time;
		for (label_t i = 0; i < EdgeLabelIndex.size(); ++i)
		{
			if (EdgeLabelIndex[i]->src_label_id == label)
			{
				std::unique_lock<std::shared_mutex> label_lock(EdgeLabelIndex[i]->mutex);
				std::unique_lock<std::shared_mutex> v_lock(
					EdgeLabelIndex[label]->VertexIndex[vertex]->mutex);
				EdgeLabelIndex[i]->VertexIndex[vertex]->deadtime = now_time;
			}
		}
	}

	void MemoryManager::PutEdge(vertex_t src, vertex_t dst, label_t label,
		edge_property_t property, time_t now_time)
	{
		std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->vertex_mutex[src]);
		auto src_entry = EdgeLabelIndex[label]->VertexIndex[src];
		if (property == TOMBSTONE)
			EdgeLabelIndex[label]->query_counter.AddDeletion(src);
		else
			EdgeLabelIndex[label]->query_counter.AddWrite(src);
		table_lock.unlock();

		std::unique_lock<std::shared_mutex> src_lock(src_entry->mutex);
		edge_t found = find_edge(src, dst, src_entry);
		if (found != NONEINDEX)
		{
			src_entry->EdgeIndex.remove(
				util::make_vertex_edge_pair(dst, found));
		}
		src_entry->EdgePool.emplace_back(dst, property, now_time, found);
		src_entry->EdgeIndex.insert(
			util::make_vertex_edge_pair(dst, src_entry->EdgePool.size() - 1));
		src_entry->size_info->max_time = std::max(now_time,
			src_entry->size_info->max_time);
		src_entry->size_info->size += sizeof(EdgeEntry);
		bool FALSE = false;
		if (src_entry->size_info->size >= db->options->MEM_TABLE_MAX_SIZE)
			if (src_entry->size_info->immutable.compare_exchange_weak(
				FALSE, true, std::memory_order_acq_rel))
			{
				immute_memtable(src_entry->size_info, label);
			}
	}
	vertex_t MemoryManager::GetVertexNum(label_t label_id) const
	{
		return VertexLabelIndex[label_id]->total_vertex;
	}
	void MemoryManager::DelEdge(vertex_t src, vertex_t dst,
		label_t label, time_t now_time)
	{
		PutEdge(src, dst, label, TOMBSTONE, now_time);
	}
	edge_property_t MemoryManager::GetEdge(vertex_t src, vertex_t dst,
		label_t label, time_t now_time)
	{
		std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->vertex_mutex[src]);
		auto src_entry = EdgeLabelIndex[label]->VertexIndex[src];
		EdgeLabelIndex[label]->query_counter.AddRead(src);
		table_lock.unlock();
		while (src_entry != NULL)
		{
			std::shared_lock<std::shared_mutex> src_lock(src_entry->mutex);
			edge_t found = find_edge(src, dst, src_entry);
			while (found != NONEINDEX)
			{
				if (src_entry->EdgePool[found].time <= now_time)
					return src_entry->EdgePool[found].property;
				found = src_entry->EdgePool[found].last_version;
			}
			src_entry = src_entry->next;
		}
		return NOTFOUND;
	}
	void MemoryManager::GetEdges(vertex_t src, label_t label, time_t now_time,
		std::shared_ptr<std::vector<
		std::tuple<vertex_t, vertex_t, edge_property_t>>> answer,
		bool (*func)(edge_property_t))
	{
		std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->vertex_mutex[src]);
		auto src_entry = EdgeLabelIndex[label]->VertexIndex[src];
		EdgeLabelIndex[label]->query_counter.AddRead(src);
		table_lock.unlock();
		while (src_entry != NULL)
		{
			std::shared_lock<std::shared_mutex> src_lock(src_entry->mutex);
			for (auto& i : src_entry->EdgePool)
			{
				if (i.time > now_time)
					continue;
				if (i.property == TOMBSTONE || func(i.property))
					answer->emplace_back(i.dst, answer->size(), i.property);
			}
			src_entry = src_entry->next;
		}
	}
	//1: leveling 2:tiering
	size_t MemoryManager::GetMergeType(label_t label_id,
		vertex_t src_b, vertex_t src_e)
	{
		return EdgeLabelIndex[label_id]->query_counter.GetCompactionType(src_e, src_e);
	}
	time_t MemoryManager::GetVertexDelTime(label_t edge_label_id, vertex_t src) const
	{
		std::shared_lock<std::shared_mutex> table_lock(
			EdgeLabelIndex[edge_label_id]->vertex_mutex[src]);
		return EdgeLabelIndex[edge_label_id]->VertexIndex[src]->deadtime;
	}
	VersionEdit* MemoryManager::MemTablePersistence(label_t label_id,
		idx_t file_id, SizeEntry* size_info, identify_t identify)
	{
		auto temp_file_metadata = new FileMetaData(label_id,
			0, size_info->begin_vertex_id, file_id,
			db->Labels->GetEdgeLabel(label_id), identify);
		std::string file_name = temp_file_metadata->file_name;
		auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/" + file_name, false);
		auto sst = std::make_shared<SSTableBuilder>(fw, db->options);
		sst->SetSrcRange(size_info->begin_vertex_id,
			size_info->begin_vertex_id + size_info->entry.size() - 1);
		for (vertex_t index = 0; index < size_info->entry.size(); ++index)
		{
			for (auto x : size_info->entry[index]->EdgeIndex)
			{
				if (x == 0)
					continue;
				auto v = util::unzip_pair_second(x);
				if (size_info->entry[index]->EdgePool[v].property == TOMBSTONE
					&& size_info->entry[index]->EdgePool[v].last_version != NONEINDEX)
					continue;
				else
					sst->AddEdge(index, util::unzip_pair_first(x),
						size_info->entry[index]->EdgePool[v].property);
				/*bool tombstone = false;
				for (auto v = util::unzip_pair_second(x); v != NONEINDEX;
					v = size_info->entry[index]->EdgePool[v].last_version)
				{
					if (size_info->entry[index]->EdgePool[v].property == TOMBSTONE)
						tombstone = true;
					else
					{
						if (tombstone == true)
							tombstone = false;
						else
						{
							sst->AddEdge(index,
								size_info->entry[index]->EdgePool[v].dst,
								size_info->entry[index]->EdgePool[v].property);
						}
					}
				}*/
			}
			sst->ArrangeCurrentSrcInfo();
		}
		sst->ArrangeSSTableInfo();
		auto vedit = new VersionEdit();
		temp_file_metadata->file_size = fw->file_size();
		vedit->EditFileList.push_back(*temp_file_metadata);
		return vedit;
	}

	void MemoryManager::vertex_property_persistence(label_t label_id)
	{
		std::string file_name = db->options->STORAGE_DIR + "/"
			+ static_cast<std::string>(db->Labels->GetVertexLabel(label_id)) + "_"
			+ std::to_string(VertexLabelIndex[label_id]->unpersistence) + ".property";
		auto fw = std::make_shared<FileWriter>(file_name, false);
		auto pf = std::make_shared<PropertyFileBuilder>(fw);
		for (auto& i : VertexLabelIndex[label_id]->VertexProperty)
		{
			pf->AddProperty(i);
		}
		pf->FinishFile();
		VertexLabelIndex[label_id]->property_size = 0;
		VertexLabelIndex[label_id]->FileIndex.push_back(
			VertexLabelIndex[label_id]->unpersistence);
		VertexLabelIndex[label_id]->unpersistence +=
			VertexLabelIndex[label_id]->VertexProperty.size();
		VertexLabelIndex[label_id]->VertexProperty.clear();
	}
	void MemoryManager::immute_memtable(SizeEntry*& size_info, label_t label)
	{
		std::unique_lock<std::shared_mutex>locks[db->options->MERGE_NUM];
		for (vertex_t i = size_info->begin_vertex_id;
			i <= size_info->begin_vertex_id + size_info->entry.size() - 1;
			++i)
		{
			locks[i - size_info->begin_vertex_id] =
				std::unique_lock<std::shared_mutex>(
					EdgeLabelIndex[label]->vertex_mutex[i]);
		}
		auto new_size_info = new SizeEntry(
			size_info->begin_vertex_id, size_info);
		if (size_info == EdgeLabelIndex[label]->now_size_info)
		{
			EdgeLabelIndex[label]->now_size_info = new_size_info;
		}
		for (vertex_t i = size_info->begin_vertex_id;
			i <= size_info->begin_vertex_id + size_info->entry.size() - 1;
			++i)
		{
			EdgeLabelIndex[label]->VertexIndex[i] =
				new VertexEntry(new_size_info,
					EdgeLabelIndex[label]->VertexIndex[i]);
		}
		Compaction x;
		x.label_id = label;
		x.target_level = 0;
		x.vertex_id_b = size_info->begin_vertex_id;
		x.Persistence = size_info;
		db->Files->AddCompaction(x);
	}
	edge_t MemoryManager::find_edge(vertex_t src, vertex_t dst, VertexEntry* entry)
	{
		auto edge_index_iter = entry->EdgeIndex.
			lower_bound(util::make_vertex_edge_pair(dst, 0));
		if (util::unzip_pair_first(*edge_index_iter) != dst)
			return NONEINDEX;
		else
			return util::unzip_pair_second(*edge_index_iter);
	}
}