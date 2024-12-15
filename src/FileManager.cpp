#include "BACH/sstable/FileManager.h"
#include "BACH/db/DB.h"

namespace BACH {
	FileManager::FileManager(DB* _db) :db(_db) {}

	void FileManager::AddCompaction(Compaction& compaction)
	{
		std::unique_lock<std::mutex> lock(CompactionCVMutex);
		CompactionList.push(compaction);
		CompactionCV.notify_one();
	}

	struct SingelEdgeInformation {
		vertex_t src_id, dst_id;
		edge_property_t prop;
		size_t parser_id;
		int16_t file_id;
		friend bool operator < (SingelEdgeInformation a, SingelEdgeInformation b)
		{
			if (a.src_id == b.src_id) {
				return a.dst_id != b.dst_id ? a.dst_id > b.dst_id : a.file_id < b.file_id;
			}
			return a.src_id > b.src_id;
		}
	};

	//把多个文件归并后生成一个新的文件，然后生成新的Version并将current_version指向这个新的version，然后旧的version如果ref为0就删除这个version并将这个version对应的文件的ref减1，如果文件ref为0则物理删除
	VersionEdit* FileManager::MergeSSTable(Compaction& compaction) {
		std::vector<SSTableParser> parsers;
		std::vector<int16_t> file_ids;
		for (auto& file : compaction.file_list)
		{
			auto reader = db->ReaderCaches->find(file);
			parsers.emplace_back(compaction.label_id,
				reader, db->options);
			if (file->level == compaction.target_level)
				file_ids.push_back(-db->options->FILE_MERGE_NUM - 10 + file->file_id);
			else
				file_ids.push_back(file->file_id + 1);
		}

		edge_num_t new_file_edge_num = 0;
		std::priority_queue<SingelEdgeInformation> q;
		vertex_t new_file_src_begin = compaction.vertex_id_b;
		for (size_t i = 0; i < parsers.size(); i++)
		{
			new_file_edge_num += parsers[i].GetEdgeNum();
			if (parsers[i].GetFirstEdge())
			{
				q.push(SingelEdgeInformation{ parsers[i].GetNowEdgeSrc(), parsers[i].GetNowEdgeDst(), parsers[i].GetNowEdgeProp(), (size_t)i, file_ids[i] });
			}
		}
		auto label_id = compaction.label_id;
		auto file_id = compaction.file_id;
		auto temp_file_metadata = new FileMetaData(label_id,
			compaction.target_level, compaction.vertex_id_b,
			file_id, db->Labels->GetEdgeLabel(label_id));
		std::string file_name = temp_file_metadata->file_name;
		auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/"
			+ file_name);
		auto sst_builder = new SSTableBuilder(fw, db->options);
		// 归并
		// 归并的时候如果碰到两个或者多个相同的边，只保留file_id最大的边；墓碑标记不能消掉，除非新生成的文件在最后一层了
		vertex_t now_src_vertex_id = new_file_src_begin;
		edge_num_t already_written_in_edge_num = 0;
		vertex_t last_src_id = -1, last_dst_id = -1;
		while (already_written_in_edge_num < new_file_edge_num) {
			SingelEdgeInformation tmp = q.top();
			q.pop();
			while (tmp.src_id != now_src_vertex_id) {
				sst_builder->ArrangeCurrentSrcInfo();
				now_src_vertex_id++;
			}
			if (last_src_id != tmp.src_id || last_dst_id != tmp.dst_id) {
				sst_builder->AddEdge(tmp.src_id, tmp.dst_id, tmp.prop);
				last_src_id = tmp.src_id;
				last_dst_id = tmp.dst_id;
			}
			already_written_in_edge_num++;
			if (!parsers[tmp.parser_id].GetNextEdge())
			{
				continue;
			}
			q.push(SingelEdgeInformation{
				parsers[tmp.parser_id].GetNowEdgeSrc(),
				parsers[tmp.parser_id].GetNowEdgeDst(),
				parsers[tmp.parser_id].GetNowEdgeProp(),
				tmp.parser_id,
				file_ids[tmp.parser_id]
				});
		}
		//while (now_src_vertex_id != new_file_src_end) {
		//	sst_builder->ArrangeCurrentSrcInfo();
		//	now_src_vertex_id++;
		//}
		sst_builder->ArrangeCurrentSrcInfo();
		sst_builder->SetSrcRange(new_file_src_begin, now_src_vertex_id);
		temp_file_metadata->filter = sst_builder->ArrangeSSTableInfo();
		delete sst_builder;
		temp_file_metadata->file_size = fw->file_size();

		VersionEdit* edit = new VersionEdit();
		edit->EditFileList.push_back(std::move(*temp_file_metadata));
		for (auto& file : compaction.file_list)
		{
			edit->EditFileList.push_back(std::move(*file));
			edit->EditFileList.back().deletion = true;
		}
		return edit;
	}
	idx_t FileManager::GetFileID(
		label_t label, idx_t level, vertex_t src_b)
	{
		auto x = src_b / util::ClacFileSize(db->options, level);
		if (FileNumList.size() <= label)
			FileNumList.resize(label + 1);
		if (FileNumList[label].size() <= level)
			FileNumList[label].resize(level + 1);
		if (FileNumList[label][level].size() <= x)
			FileNumList[label][level].resize(x + 1, 0);
		return FileNumList[label][level][x]++;
	}
}