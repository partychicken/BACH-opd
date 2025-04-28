#include "BACH/sstable/RelFileManager.h"
#include "BACH/db/DB.h"

namespace BACH {
    RelFileManager::RelFileManager(DB *_db) : db(_db) {
    }

    void RelFileManager::AddCompaction(RelCompaction<std::string> &compaction) {
        std::unique_lock<std::mutex> lock(CompactionCVMutex);
        CompactionList.push(compaction);
        CompactionCV.notify_one();
    }

    template<typename Key_t>
    struct TupleMessage {
        Key_t key;
        idx_t offset;
        int file_idx;

        bool operator <(const TupleMessage &other) const {
            return key == other.key ? file_idx < other.file_idx : key < other.key;
        }

        bool operator == (const TupleMessage &other) const {
            return key == other.key && file_idx == other.file_idx;
        }

        TupleMessage(Key_t key, idx_t offset, int file_idx) : key(key), offset(offset), file_idx(file_idx) {
        }
    };

    template<typename Key_t>
    struct Compare {
        bool operator()(const TupleMessage<Key_t>& a, const TupleMessage<Key_t>& b) {
            return b < a; // 当 b < a 时返回 true，使优先队列按小根堆排序
        }
    };

    struct DictMappingEntry {
        std::string key;
        std::vector<std::pair<int, idx_t> > us; //<file_id, origin_index>
        bool operator <(const DictMappingEntry &other) const {
            return key < other.key;
        }

        bool operator ==(const DictMappingEntry &other) const {
            return key == other.key;
        }
    };


    //把多个文件归并后生成一个新的文件，然后生成新的Version并将current_version指向这个新的version，然后旧的version如果ref为0就删除这个version并将这个version对应的文件的ref减1，如果文件ref为0则物理删除
    VersionEdit *RelFileManager::MergeRelFile(Compaction &compaction) {
        std::vector<RelFileParser<std::string> > parsers;
        std::vector<int16_t> file_ids;
        for (auto &file: compaction.file_list) {
            auto reader = db->ReaderCaches->find(file);
            parsers.emplace_back(reader, db->options, file->file_size);
            DictList = &static_cast<RelFileMetaData<std::string> *>(file)->dictionary;
            if (file->level == compaction.target_level)
                file_ids.push_back(-db->options->FILE_MERGE_NUM - 10 + file->file_id);
            else
                file_ids.push_back(file->file_id + 1);
        }

        int file_num = file_ids.size();
        idx_t col_num = parsers.begin()->GetColumnNum(); // not check the consistency among files

        std::priority_queue<TupleMessage<std::string>, std::vector<TupleMessage<std::string> >,
            Compare<std::string> > q;
        std::string new_file_key_min = static_cast<RelCompaction<std::string>>(compaction).key_min;

        std::string *keys[file_num];
        idx_t *vals[file_num][col_num];
        int key_num[file_num], now_idx[file_num];
        int key_tot_num = 0, key_done = 0;
        memset(now_idx, 0, sizeof(now_idx));
        for (size_t i = 0; i < parsers.size(); i++) {
            keys[i] = static_cast<std::string *>(malloc(sizeof(key_t) * parsers[i].GetKeyNum()));
            idx_t check_size = 0;
            parsers[i].GetKeyCol(keys[i], check_size);
            if (check_size != parsers[i].GetKeyNum()) {
                return nullptr;
            }
            key_tot_num += (key_num[i] = check_size);

            for (idx_t j = 0; j < col_num; j++) {
                vals[i][j] = static_cast<idx_t *>(malloc(sizeof(idx_t) * parsers[i].GetKeyNum()));
                parsers[i].GetValCol(vals[i][j], check_size, j);
            }
            q.push(TupleMessage<std::string>(keys[i][0], 0, i));
        }


        auto temp_file_metadata = new RelFileMetaData<std::string>(0, compaction.target_level, compaction.vertex_id_b,
                                                             compaction.file_id, "rel", new_file_key_min, "", 0, 0);
        std::string file_name = temp_file_metadata->file_name;
        auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/"
                                               + file_name);
        auto rel_builder = new RelFileBuilder<std::string>(fw, db->options);

        std::string *order_key_buf = static_cast<std::string *>(malloc(sizeof(std::string) * (key_tot_num / file_num + 5)));
        int key_buf_idx = 0;
        std::pair<idx_t, idx_t> *val_buf[col_num];
        for (idx_t i = 0; i < col_num; i++) {
            val_buf[i] = static_cast<std::pair<idx_t, idx_t> *>(malloc(
                sizeof(std::pair<idx_t, idx_t>) * (key_tot_num / file_num + 5)));
        }

        idx_t *real_val_buf[col_num];
        for (idx_t i = 0; i < col_num; i++) {
            real_val_buf[i] = static_cast<idx_t *>(malloc(sizeof(idx_t) * (key_tot_num / file_num + 5)));
        }

        VersionEdit *edit = new VersionEdit();

        //std::set<DictMappingEntry> s[col_num];
        std::map<std::string, std::vector<std::pair<int, idx_t> >> s[col_num]; //<file_id, origin_index>
        int remap[col_num][file_num][key_tot_num]; //third dimension is larger than the necessary's, needing optimized
        // in the first stage, remap denotes whether the index exists in set;
        // in the second stage, remap denotes the new index that the old one should be mapped to
        memset(remap, 0, sizeof(remap));

        std::string last_key = 0;
        idx_t now_file_id = compaction.file_id;

        while (!q.empty()) {
            TupleMessage<std::string> now_message = q.top();
            q.pop();

            if (now_message.offset < key_num[now_message.file_idx]) {
                q.push(TupleMessage<std::string>(keys[now_message.file_idx][now_message.offset + 1],
                                           now_message.offset + 1, now_message.file_idx));
            }

            if (now_message.key == last_key) {
                continue;
            }
            last_key = now_message.key;

            order_key_buf[key_buf_idx] = now_message.key;
            for (idx_t i = 0; i < col_num; i++) {
                val_buf[i][key_buf_idx] = std::make_pair(now_message.file_idx,
                                                    vals[now_message.file_idx][i][now_message.offset]);
                if (!remap[i][now_message.file_idx][now_message.offset]) {
                    remap[i][now_message.file_idx][now_message.offset] = 1;
                    auto nowstr = (*DictList)[i].getString(val_buf[i][key_buf_idx].second);
                    auto it = s[i].lower_bound(nowstr);
                    if (it != s[i].end() && it->first == nowstr) {
                        it->second.push_back(std::make_pair(now_message.file_idx, now_message.offset));
                    } else {
                        std::vector<std::pair<int, idx_t> > tmp;
                        tmp.push_back(std::make_pair(now_message.file_idx, now_message.offset));
                        s[i].insert(std::make_pair(nowstr, tmp));
                    }
                }
            }
            key_buf_idx++;

            if (key_buf_idx * file_num >= key_tot_num) {
                //flush buf to a new file
                //build new dict
                int nowidx = 0;
                for (idx_t i = 0; i < col_num; i++) {
                    std::vector<std::string> dict;
                    for (auto entry: s[i]) {
                        dict.emplace_back(entry.first);
                        for (auto p: entry.second) {
                            remap[i][p.first][p.second] = nowidx;
                        }
                    }
                    temp_file_metadata->dictionary.push_back(OrderedDictionary(dict));
                }
                //map new index from new dictionary
                for (idx_t i = 0; i < col_num; i++) {
                    for (int j = 0; j < key_buf_idx; j++) {
                        auto p = val_buf[i][j];
                        real_val_buf[i][j] = remap[i][p.first][p.second];
                    }
                }

                //write current buffer to file
                rel_builder->ArrangeRelFileInfo(order_key_buf, key_buf_idx, db->options->KEY_SIZE, col_num, real_val_buf);
                temp_file_metadata->key_max = last_key;
                temp_file_metadata->key_num = key_buf_idx;
                temp_file_metadata->col_num = col_num;
                edit->EditFileList.push_back(temp_file_metadata);

                //reset buffer information
                key_buf_idx = 0;
                memset(remap, 0, sizeof(remap));
                for (idx_t i = 0; i < col_num; i++) {
                    s[i].clear();
                }

                //open new file
                temp_file_metadata = new RelFileMetaData<std::string>(0, compaction.target_level, compaction.vertex_id_b,
                                                                     ++now_file_id, "rel", q.top().key, "", 0, 0);
                std::string file_name = temp_file_metadata->file_name;
                fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/"
                                                       + file_name);
                delete rel_builder;
                rel_builder = new RelFileBuilder<std::string>(fw, db->options);
            }
        }

        for (auto &file: compaction.file_list) {
            edit->EditFileList.push_back(file);
            edit->EditFileList.back()->deletion = true;
        }
        return edit;
    }
}
