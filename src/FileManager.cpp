#include "BACH/sstable/FileManager.h"
#include "queue"

namespace BACH {
    void FileManager::AddCompaction(Compaction& compaction)
	{
		std::unique_lock<std::mutex> lock(CompactionCVMutex);
		CompactionList.push(compaction);
		CompactionCV.notify_one();
	}
    std::pair<std::string, std::shared_ptr<FileMetaData>>
		FileManager::NewSSTable(label_t label_id, idx_t level,
			vertex_t vertex_id_b, vertex_t vertex_id_e, time_t time)
	{
		std::unique_lock<std::shared_mutex>lock(FileIndexMutex);
		if (FileIndex[label_id].size() <= level)
			FileIndex[label_id].emplace_back();
		auto iter = levelindex(label_id, level).lower_bound(std::make_pair(vertex_id_b, NONEINDEX));
		idx_t file_id;
		if (iter->first.first == vertex_id_b)
		{
			file_id = iter->first.second + 1;
		}
		else
		{
			file_id = 0;
		}
		std::string_view label = db->Labels->GetEdgeLabel(label_id);
		auto temp_file_metadata = std::make_shared<FileMetaData>(label,
			level, vertex_id_b, vertex_id_e, file_id, time);
		levelindex(label_id, level).emplace(std::make_pair(vertex_id_b, file_id),
			*temp_file_metadata);
		return std::make_pair(util::BuildSSTPath(label, level, vertex_id_b, file_id),
			temp_file_metadata);
	}

    void FileManager::MergeSSTable(Compaction& compaction, time_t now_time) {
        //把多个文件归并后生成一个新的文件，然后生成新的Version并将current_version指向这个新的version，然后旧的version如果ref为0就删除这个version并将这个version对应的文件的ref减1，如果文件ref为0则物理删除
        std::vector<SSTableParser> parsers;
        for (auto& file : compaction.file_list)
		{
			parsers.emplace_back(db, compaction.label_id,
				std::make_shared<FileReader>(
					db->options->STORAGE_DIR + "/" + file.file_name()),
				db->options, false);
		}
        auto file_info = NewSSTable(compaction.label_id, compaction.target_level,
			compaction.vertex_id_b, compaction.vertex_id_e,now_time);
        std::string file_name = db->options->STORAGE_DIR + "/"
			+ static_cast<std::string>(file_info.first);
        auto writer = std::make_shared<FileWriter>(file_name);
        auto dst_entry_buffer = std::make_shared<WriteBuffer<DstEntry, vertex_t>>(
			file_name + ".temp", db->options);
        for (auto& parser : parsers)
		{
			parser.BatchReadEdge();
            //现在假设每个文件中都读出了一部分数据，数据格式为(src_vertex_id,dst_vertex_id,edge_property)的三元组
            //因此BatchReadEdge需要在读取部分CSR块后转成三元组形式
            //或者是一种部分CSR块的形式
		}
        // 归并
        priority_queue<unsigned long>q;
        for (auto& parser : parsers)
		{
			//todo()! 返回第一条边并生成三元组tuple
            q.push(_tuple);
		} 
        while (true)
        {
            int file_list_len = compaction.file_list.size();
            for(int i = 0; i < file_list_len;i++) {
                //有buffer，需要重改
            }
        }
        // create new version
        current_version = _new_version;
        //check ref in version 
        // if old version ref is 0 , execute delete_version in Version Class
    }
}