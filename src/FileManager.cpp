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
		friend bool operator < (SingelEdgeInformation a, SingelEdgeInformation b)
		{
			if (a.src_id == b.src_id) {
				return a.dst_id > b.dst_id;
			}
			return a.src_id > b.src_id;
		}
	};

	VersionEdit* FileManager::MergeSSTable(Compaction& compaction) {
		//把多个文件归并后生成一个新的文件，然后生成新的Version并将current_version指向这个新的version，然后旧的version如果ref为0就删除这个version并将这个version对应的文件的ref减1，如果文件ref为0则物理删除
		std::vector<SSTableParser> parsers;
		for (auto& file : compaction.file_list)
		{
			parsers.emplace_back(db, compaction.label_id,
				std::make_shared<FileReader>(
					db->options->STORAGE_DIR + "/" + file.file_name),
				db->options, false);
		}
		auto file_info = new FileMetaData(compaction.label_id, compaction.target_level,
			compaction.vertex_id_b, compaction.file_id,
			db->Labels->GetEdgeLabel(compaction.label_id));

		std::string file_name = db->options->STORAGE_DIR + "/"
			+ file_info->file_name;
		auto writer = std::make_shared<FileWriter>(file_name);
		auto new_file_edge_num = 0;
		std::priority_queue<SingelEdgeInformation> q;
		vertex_t new_file_src_begin = -1, new_file_src_end = 0;
		for (auto i = 0; i < parsers.size(); i++)
		{
			new_file_edge_num += parsers[i].GetEdgeNum();
			new_file_src_begin = std::min(new_file_src_begin, parsers[i].GetSrcBegin());
			new_file_src_end = std::max(new_file_src_end, parsers[i].GetSrcEnd());
			if (parsers[i].GetFirstEdge())
			{
				q.push(SingelEdgeInformation{parsers[i].GetNowEdgeSrc(), parsers[i].GetNowEdgeDst(), parsers[i].GetNowEdgeProp(), i});
			}
		}
		auto label_id = compaction.label_id;
		idx_t file_id = db->Files->GetFileID(
			label_id, compaction.target_level, compaction.vertex_id_b);
		auto temp_file_metadata = new FileMetaData(label_id,
			compaction.target_level, compaction.vertex_id_b, file_id, db->Labels->GetEdgeLabel(label_id));
		std::string file_name = temp_file_metadata->file_name;
		auto fw = std::make_shared<FileWriter>(file_name, false);
		auto sst_builder = std::make_shared<SSTableBuilder>(fw);
		sst_builder->SetSrcRange(new_file_src_begin, new_file_src_end);
		// 归并
		vertex_t now_src_vertex_id = new_file_src_begin;
		edge_num_t already_written_in_edge_num = 0;
		while (already_written_in_edge_num < new_file_edge_num) {
			SingelEdgeInformation tmp = q.pop();
			while(tmp.src_id != now_vertex_id) {
				sst_builder->ArrangeCurrentSrcInfo();
				now_src_vertex_id ++;
			}
			sst_builder->AddEdge(tmp.src_id,tmp.dst_id,tmp.prop);
			already_written_in_edge_num ++;
			if(!parsers[tmp.parser_id].GetNextEdge())
			{
				continue;
			}
			q.push(SingelEdgeInformation{
				parsers[tmp.parser_id].GetNowEdgeSrc(),
				parsers[tmp.parser_id].GetNowEdgeDst(),
				parsers[tmp.parser_id].GetNowEdgeProp(),
				tmp.parser_id
				});
		}
		while (now_src_vertex_id != new_file_src_end) {
			sst_builder->ArrangeCurrentSrcInfo();
			now_src_vertex_id ++;
		}
		sst_builder->ArrangeSSTableInfo();
		while (true)
		{
			int file_list_len = compaction.file_list.size();
			for (int i = 0; i < file_list_len; i++) {
				//有buffer，需要重改
			}
		}
		VersionEdit* edit = new VersionEdit();
		edit->EditFileList.push_back(*file_info);
		for (auto& file : compaction.file_list)
		{
			file.deletion = true;
			edit->EditFileList.push_back(file);
		}
		return edit;
	}
	idx_t FileManager::GetFileID(label_t label, idx_t level, vertex_t src_b)
	{
		auto x = src_b / util::ClacFileSize(db->options->MERGE_NUM, level);
		if (FileNumList.size() <= label)
			FileNumList.resize(label + 1);
		if (FileNumList[label].size() <= level)
			FileNumList[label].resize(level + 1);
		if (FileNumList[label][level].size() <= x)
			FileNumList[label][level].resize(x + 1);
		return FileNumList[label][level][x]++;
	}
}