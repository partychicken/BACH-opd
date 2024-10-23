#include "BACH/memory/MemoryManager.h"
#include "BACH/db/DB.h"

namespace BACH
{
	MemoryManager::MemoryManager(DB* _db) :
		db(_db), writing_size(std::make_shared<SizeEntry>(-1, 0)),
		writing_vertex(std::make_shared<VertexEntry>(writing_size))
	{

	}

	void MemoryManager::AddVertexLabel()
	{
		VertexLabelIndex.push_back(new VertexLabelEntry());
	}
	void MemoryManager::AddEdgeLabel(label_t src_label, label_t dst_label)
	{
		EdgeLabelIndex.push_back(new EdgeLabelEntry(db->options, src_label));
	}

	vertex_t MemoryManager::AddVertex(label_t label_id)
	{
		std::unique_lock<std::shared_mutex> lock(VertexLabelIndex[label_id]->mutex);
		vertex_t vertex_id = VertexLabelIndex[label_id]->total_vertex.fetch_add(1);
		VertexLabelIndex[label_id]->PropertyID.push_back(-1);
		for (label_t i = 0; i < EdgeLabelIndex.size(); ++i)
		{
			if (EdgeLabelIndex[i]->src_label_id == label_id)
			{
				std::unique_lock<std::shared_mutex> lock(EdgeLabelIndex[i]->mutex);
				while (EdgeLabelIndex[i]->SizeIndex.size() <= vertex_id / db->options->MEMORY_MERGE_NUM)
					EdgeLabelIndex[i]->SizeIndex.emplace_back_default(),
					EdgeLabelIndex[i]->size_mutex.emplace_back_default();
				EdgeLabelIndex[i]->query_counter.AddVertex();
			}
		}
		return vertex_id;
	}
	void MemoryManager::PutVertex(label_t label_id, vertex_t vertex_id,
		std::string_view property)
	{
		std::unique_lock<std::shared_mutex> lock(VertexLabelIndex[label_id]->mutex);
		vertex_t property_id = VertexLabelIndex[label_id]->total_property.fetch_add(1);
		VertexLabelIndex[label_id]->VertexProperty.push_back(
			static_cast<std::string>(property));
		VertexLabelIndex[label_id]->PropertyID[vertex_id] = property_id;
		VertexLabelIndex[label_id]->property_size += property.size();
		if (VertexLabelIndex[label_id]->property_size >=
			db->options->VERTEX_PROPERTY_MAX_SIZE)
		{
			vertex_property_persistence(label_id);
		}
	}
	std::shared_ptr<std::string> MemoryManager::GetVertex(vertex_t vertex, label_t label,
		time_t now_time)
	{
		//std::shared_lock<std::shared_mutex> lock(VertexLabelIndex[label]->mutex);
		auto x = VertexLabelIndex[label]->deletetime.find(vertex);
		if (x == VertexLabelIndex[label]->deletetime.end()
			|| x->second > now_time)
		{
			auto property_id = VertexLabelIndex[label]->PropertyID[vertex];
			if (property_id >= VertexLabelIndex[label]->unpersistence)
			{
				return std::make_shared<std::string>(
					VertexLabelIndex[label]->VertexProperty[
						property_id - VertexLabelIndex[label]->unpersistence]);
			}
			else
			{
				idx_t file_no = VertexLabelIndex[label]->FileIndex.rlowerbound(property_id);
				auto reader = std::make_shared<FileReader>(
					db->options->STORAGE_DIR + "/" + static_cast<std::string>(
						db->Labels->GetVertexLabel(label)) + "_"
					+ std::to_string(file_no) + ".property");
				auto parser = std::make_shared<PropertyFileParser>(
					reader);
				return parser->GetProperty(property_id - file_no);
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
				auto k = vertex / db->options->MEMORY_MERGE_NUM;
				auto index = vertex - k * db->options->MEMORY_MERGE_NUM;
				std::shared_ptr<SizeEntry> size_entry;
				std::shared_ptr<VertexEntry> src_entry;
				do
				{
					size_entry = EdgeLabelIndex[label]->SizeIndex[k];
				} while (size_entry == writing_size);
				if (size_entry == NULL)
					return;
				do
				{
					src_entry = size_entry->entry[index];
				} while (src_entry == writing_vertex);
				if (src_entry == NULL)
					return;
				/*auto size_entry = EdgeLabelIndex[label]->SizeIndex[k];
				if (size_entry == NULL)
					return;
				auto src_entry = size_entry->entry[index];
				if (src_entry == NULL)
					return;*/
				std::unique_lock<std::shared_mutex> v_lock(
					src_entry->mutex);
				src_entry->deadtime = now_time;
			}
		}
	}

	void MemoryManager::PutEdge(vertex_t src, vertex_t dst, label_t label,
		edge_property_t property, time_t now_time)
	{
		if (property == TOMBSTONE)
			EdgeLabelIndex[label]->query_counter.AddDeletion(src);
		else
			EdgeLabelIndex[label]->query_counter.AddWrite(src);
		auto k = src / db->options->MEMORY_MERGE_NUM;
		auto index = src - k * db->options->MEMORY_MERGE_NUM;
		std::shared_ptr<SizeEntry> size_entry;
		std::shared_ptr<VertexEntry> src_entry;
	RETRY:
		std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->size_mutex[k]);
		do
		{
			size_entry = NULL;
			if (EdgeLabelIndex[label]->SizeIndex[k].compare_exchange_weak(
				size_entry, writing_size))
			{
				EdgeLabelIndex[label]->SizeIndex[k].store(
					size_entry =
					std::make_shared<SizeEntry>(k, db->options->MEMORY_MERGE_NUM));
			}
		} while (size_entry == NULL || size_entry == writing_size);
		do
		{
			src_entry = NULL;
			if (size_entry->entry[index].compare_exchange_weak(
				src_entry, writing_vertex))
			{
				size_entry->entry[index].store(
					src_entry =
					std::make_shared<VertexEntry>(size_entry));
			}
		} while (src_entry == NULL || src_entry == writing_vertex);
		//	std::unique_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->size_mutex[k]);
		//	auto size_entry = EdgeLabelIndex[label]->SizeIndex[k];
		//	if (size_entry == NULL)
		//	{
		//		size_entry = EdgeLabelIndex[label]->SizeIndex[k] =
		//			std::make_shared<SizeEntry>(k, db->options->MEMORY_MERGE_NUM);
		//	}
		//	auto src_entry = size_entry->entry[index];
		//	if (src_entry == NULL)
		//	{
		//		src_entry = size_entry->entry[index] =
		//			std::make_shared<VertexEntry>(size_entry);
		//	}
		table_lock.unlock();
		std::unique_lock<std::shared_mutex> src_lock(src_entry->mutex);
		if (size_entry->immutable)
		{
			size_entry->sema.try_acquire();
			goto RETRY;
		}
		edge_t found = find_edge(src, dst, src_entry);
		src_entry->EdgeIndex[dst] = src_entry->EdgePool.size();
		src_entry->EdgePool.emplace_back(dst, property, now_time, found);
		src_entry->size_info->max_time = std::max(now_time,
			src_entry->size_info->max_time);
		src_entry->size_info->size += sizeof(EdgeEntry);
		bool FALSE = false;
		if (src_entry->size_info->size >= db->options->MEM_TABLE_MAX_SIZE)
			if (src_entry->size_info->immutable.compare_exchange_weak(
				FALSE, true, std::memory_order_acq_rel))
			{
				src_lock.unlock();
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
		EdgeLabelIndex[label]->query_counter.AddRead(src);
		auto k = src / db->options->MEMORY_MERGE_NUM;
		auto index = src - k * db->options->MEMORY_MERGE_NUM;
		std::shared_ptr<SizeEntry> size_entry;
		std::shared_ptr<VertexEntry> src_entry;
		do
		{
			size_entry = EdgeLabelIndex[label]->SizeIndex[k];
		} while (size_entry == writing_size);
		if (size_entry == NULL)
			return NOTFOUND;
		do
		{
			src_entry = size_entry->entry[index];
		} while (src_entry == writing_vertex);

		//std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->size_mutex[k]);
		//auto size_entry = EdgeLabelIndex[label]->SizeIndex[k];
		//if (size_entry == NULL)
		//	return NOTFOUND;
		//auto src_entry = size_entry->entry[index];
		//table_lock.unlock();
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
		std::pair<vertex_t, edge_property_t>>> answer_temp[],
		vertex_t& c,
		//sul::dynamic_bitset<>& filter,
		bool (*func)(edge_property_t))
	{
		EdgeLabelIndex[label]->query_counter.AddRead(src);
		auto k = src / db->options->MEMORY_MERGE_NUM;
		auto index = src - k * db->options->MEMORY_MERGE_NUM;
		std::shared_ptr<SizeEntry> size_entry;
		std::shared_ptr<VertexEntry> src_entry;
		do
		{
			size_entry = EdgeLabelIndex[label]->SizeIndex[k];
		} while (size_entry == writing_size);
		if (size_entry == NULL)
			return;
		do
		{
			src_entry = size_entry->entry[index];
		} while (src_entry == writing_vertex);
		//std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->size_mutex[k]);
		//auto size_entry = EdgeLabelIndex[label]->SizeIndex[k];
		//if (size_entry == NULL)
		//	return;
		//auto src_entry = size_entry->entry[index];
		//table_lock.unlock();
		while (src_entry != NULL)
		{
			std::shared_lock<std::shared_mutex> src_lock(src_entry->mutex);
			vertex_t answer_size = answer_temp[(c + 1) % 3]->size();
			vertex_t answer_cnt = 0;
			for (auto& i : src_entry->EdgeIndex)
			{
				auto dst = i.first;
				auto index = i.second;
				while (index != NONEINDEX &&
					src_entry->EdgePool[index].time > now_time)
					index = src_entry->EdgePool[index].last_version;
				if (index != NONEINDEX
					&& src_entry->EdgePool[index].property != TOMBSTONE
					&& func(src_entry->EdgePool[index].property))
				{
					if (answer_cnt >= answer_size)
						answer_temp[(c + 1) % 3]->emplace_back(dst, src_entry->EdgePool[index].property);
					else
					{
						(*answer_temp[(c + 1) % 3])[answer_cnt].first = dst;
						(*answer_temp[(c + 1) % 3])[answer_cnt++].second = src_entry->EdgePool[index].property;
					}
				}
				//filter[dst] = true;
			}
			if (answer_cnt < answer_size)
			{
				answer_temp[(c + 1) % 3]->resize(answer_cnt);
			}
			merge_answer(answer_temp, c);
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
		auto k = src / db->options->MEMORY_MERGE_NUM;
		auto index = src - k * db->options->MEMORY_MERGE_NUM;
		std::shared_ptr<SizeEntry> size_entry;
		std::shared_ptr<VertexEntry> src_entry;
		do
		{
			size_entry = EdgeLabelIndex[edge_label_id]->SizeIndex[k];
		} while (size_entry == writing_size);
		if (size_entry == NULL)
			return MAXTIME;
		do
		{
			src_entry = size_entry->entry[index];
		} while (src_entry == writing_vertex);
		//std::shared_lock<std::shared_mutex> table_lock(EdgeLabelIndex[edge_label_id]->size_mutex[k]);
		//auto size_entry = EdgeLabelIndex[edge_label_id]->SizeIndex[k];
		//if (size_entry == NULL)
		//	return MAXTIME;
		//auto src_entry = size_entry->entry[index];
		//table_lock.unlock();
		if (src_entry == NULL)
			return MAXTIME;
		return src_entry->deadtime;
	}
	VersionEdit* MemoryManager::MemTablePersistence(label_t label_id,
		idx_t file_id, std::shared_ptr < SizeEntry > size_info)
	{
		auto temp_file_metadata = new FileMetaData(label_id,
			0, size_info->begin_vertex_id, file_id,
			db->Labels->GetEdgeLabel(label_id));
		std::string file_name = temp_file_metadata->file_name;
		auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/" + file_name, false);
		auto sst = std::make_shared<SSTableBuilder>(fw, db->options);
		sst->SetSrcRange(size_info->begin_vertex_id,
			size_info->begin_vertex_id
			+ db->options->MEMORY_MERGE_NUM - 1);
		std::unique_lock<std::shared_mutex>locks[db->options->MEMORY_MERGE_NUM];
		for (vertex_t index = 0; index < size_info->entry.size(); ++index)
		{
			std::shared_ptr<VertexEntry> src_entry;
			do
			{
				src_entry = size_info->entry[index];
			} while (src_entry == writing_vertex);
			if (src_entry != NULL)
			{
				locks[index] = std::unique_lock<std::shared_mutex>(
					src_entry->mutex);
				for (auto& x : src_entry->EdgeIndex)
				{
					auto& v = x.second;
					if (src_entry->EdgePool[v].property == TOMBSTONE
						&& src_entry->EdgePool[v].last_version != NONEINDEX)
						continue;
					else
						sst->AddEdge(index, x.first,
							src_entry->EdgePool[v].property);
				}
			}
			sst->ArrangeCurrentSrcInfo();
		}
		temp_file_metadata->filter = sst->ArrangeSSTableInfo();
		auto vedit = new VersionEdit();
		temp_file_metadata->file_size = fw->file_size();
		vedit->EditFileList.push_back(*temp_file_metadata);
		return vedit;
	}

	void MemoryManager::merge_answer(std::shared_ptr<std::vector
		<std::pair<vertex_t, edge_property_t>>> answer_temp[3],
		vertex_t& c)
	{
		if (answer_temp[(c + 1) % 3]->size() == 0)
		{
			return;
		}
		if (answer_temp[c]->size() == 0)
		{
			c = (c + 1) % 3;
			return;
		}
		answer_temp[(c + 2) % 3]->resize(
			answer_temp[c]->size() + answer_temp[(c + 1) % 3]->size());
		//vertex_t answer_size = answer_temp[(c + 2) % 3]->size();
		vertex_t answer_cnt = 0;
		vertex_t i = 0, j = 0;
		auto put_element_in_answer =
			[&](vertex_t nc, vertex_t& cnt)
			{
				(*answer_temp[(c + 2) % 3])[answer_cnt++] =
					(*answer_temp[nc])[cnt++];
			};
		for (; i < answer_temp[c]->size() &&
			j < answer_temp[(c + 1) % 3]->size();)
			if ((*answer_temp[c])[i].first <= (*answer_temp[(c + 1) % 3])[j].first)
				put_element_in_answer(c, i);
			else
				put_element_in_answer((c + 1) % 3, j);
		while (i < answer_temp[c]->size())
			put_element_in_answer(c, i);
		while (j < answer_temp[(c + 1) % 3]->size())
			put_element_in_answer((c + 1) % 3, j);
		if (answer_cnt < answer_temp[(c + 2) % 3]->size())
		{
			answer_temp[(c + 2) % 3]->resize(answer_cnt);
		}
		c = (c + 2) % 3;
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
	void MemoryManager::immute_memtable(std::shared_ptr < SizeEntry > size_info, label_t label)
	{
		auto k = size_info->begin_vertex_id / db->options->MEMORY_MERGE_NUM;
		auto new_size_info = std::make_shared<SizeEntry>(
			k, db->options->MEMORY_MERGE_NUM);
		//std::unique_lock<std::shared_mutex> table_lock(EdgeLabelIndex[label]->size_mutex[k]);
		size_info->last = new_size_info;
		//std::unique_lock<std::shared_mutex>locks[db->options->MEMORY_MERGE_NUM];
		//for (vertex_t i = 0; i < size_info->entry.size(); ++i)
		//	if (size_info->entry[i] != NULL)
		//	{
		//		locks[i] = std::unique_lock<std::shared_mutex>(
		//			size_info->entry[i]->mutex);
		//	}
		for (vertex_t i = 0; i < size_info->entry.size(); ++i)
			if (size_info->entry[i].load() != NULL)
			{
				auto x = std::make_shared <VertexEntry>(new_size_info,
					size_info->entry[i]);
				new_size_info->entry[i] = x;
			}
		EdgeLabelIndex[label]->SizeIndex[k] = new_size_info;
		size_info->sema.release(1024);
		//table_lock.unlock();
		Compaction x;
		x.label_id = label;
		x.target_level = 0;
		x.vertex_id_b = size_info->begin_vertex_id;
		x.Persistence = size_info;
		db->Files->AddCompaction(x);
	}
	edge_t MemoryManager::find_edge(vertex_t src, vertex_t dst, std::shared_ptr < VertexEntry > entry)
	{
		auto edge_index_iter = entry->EdgeIndex.
			find(dst);
		if (edge_index_iter == entry->EdgeIndex.end())
			return NONEINDEX;
		else
			return edge_index_iter->second;
	}
}