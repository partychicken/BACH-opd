#include "BACH/sstable/RelFileManager.h"
#include "BACH/db/DB.h"

namespace BACH {
    RelFileManager::RelFileManager(DB *_db) : db(_db) {
    }

    void RelFileManager::AddCompaction(RelCompaction<std::string> &compaction, bool high) {
        if (high) {
            std::unique_lock<std::mutex> lock(HighCompactionCVMutex);
            HighCompactionList.push(compaction);
            HighCompactionCV.notify_one();
            return;
        } else {
            std::unique_lock<std::mutex> lock(LowCompactionCVMutex);
            LowCompactionList.push(compaction);
            LowCompactionCV.notify_one();
        }
    }

    template<typename Key_t>
    struct TupleMessage {
        Key_t key;
        idx_t offset;
        int file_idx;

        bool operator <(const TupleMessage &other) const {
            return key == other.key ? file_idx < other.file_idx : key < other.key;
        }

        bool operator ==(const TupleMessage &other) const {
            return key == other.key && file_idx == other.file_idx;
        }

        TupleMessage(const Key_t &_key, idx_t offset, int file_idx) : key(_key), offset(offset), file_idx(file_idx) {
        }
    };

    template<typename Key_t>
    struct Compare {
        bool operator()(const TupleMessage<Key_t> &a, const TupleMessage<Key_t> &b) {
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
        std::vector<std::vector<OrderedDictionary> *> DictList;
        //DictList = new std::vector<OrderedDictionary> *[compaction.file_list.size()];
        DictList.resize(compaction.file_list.size());
        int file_num = 0;
        for (auto &file: compaction.file_list) {
            auto reader = db->ReaderCaches->find(file);
            parsers.emplace_back(reader, db->options, file->file_size,
                                 static_cast<RelFileMetaData<std::string> *>(file));
            DictList[file_num] = &(static_cast<RelFileMetaData<std::string> *>(file)->dictionary);
            if (file->level == compaction.target_level)
                file_ids.push_back(-db->options->FILE_MERGE_NUM - 10 + file->file_id);
            else
                file_ids.push_back(file->file_id + 1);
            file_num++;
        }

        idx_t col_num = parsers.begin()->GetColumnNum(); // not check the consistency among files

        int max_val[file_num];
        for (int i = 0; i < DictList.size(); i++) {
            auto dict = DictList[i];
            for (auto x: *dict) {
                max_val[i] = std::max(max_val[i], x.getCount());
            }
        }


        std::priority_queue<TupleMessage<std::string>, std::vector<TupleMessage<std::string> >,
            Compare<std::string> > q;
        std::string new_file_key_min = static_cast<RelCompaction<std::string>>(compaction).key_min;

        std::string *keys[file_num];
        idx_t *vals[file_num][col_num];
        int key_num[file_num], now_idx[file_num];
        int key_tot_num = 0;
        memset(now_idx, 0, sizeof(now_idx));
        for (int i = 0; i < file_num; i++) {
            keys[i] = new std::string[parsers[i].GetKeyNum()];
            idx_t check_size = 0;
            parsers[i].GetKeyCol(keys[i], check_size);
            if (check_size != parsers[i].GetKeyNum()) {
                return nullptr;
            }
            key_tot_num += (key_num[i] = check_size);

            idx_t tmp = 0;
            for (idx_t j = 0; j < col_num; j++) {
                vals[i][j] = static_cast<idx_t *>(malloc(sizeof(idx_t) * check_size));
                parsers[i].GetValCol(vals[i][j], tmp, j);
            }
            q.emplace(keys[i][0], 0, i);
        }

        auto temp_file_metadata = new RelFileMetaData<std::string>(0, compaction.target_level, compaction.vertex_id_b,
                                                                   compaction.file_id, "", new_file_key_min, "", 0,
                                                                   0);
        std::string file_name = temp_file_metadata->file_name;
        auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/"
                                               + file_name);
        auto rel_builder = new RelFileBuilder<std::string>(fw, db->options);

        std::string *order_key_buf = new std::string[key_tot_num / file_num + 5];
        int key_buf_idx = 0;
        std::vector<std::pair<idx_t, idx_t> > val_buf[col_num];
        for (int i = 0; i < col_num; i++) {
            val_buf[i].resize(key_tot_num / file_num + 5);
        }

        idx_t *real_val_buf[col_num];
        for (idx_t i = 0; i < col_num; i++) {
            real_val_buf[i] = static_cast<idx_t *>(malloc(sizeof(idx_t) * (key_tot_num / file_num + 5)));
        }

        VersionEdit *edit = new VersionEdit();

        //std::set<DictMappingEntry> s[col_num];
        std::map<std::string, std::vector<std::pair<int, idx_t> > > s[col_num]; //<file_id, origin_index>
        std::vector<int> remap[col_num][file_num]; //third dimension is larger than the necessary's, needing optimized
        // in the first stage, remap denotes whether the index exists in set;
        // in the second stage, remap denotes the new index that the old one should be mapped to
        for (idx_t i = 0; i < col_num; i++) {
            for (int j = 0; j < file_num; j++) {
                remap[i][j].resize(max_val[j] + 5);
            }
        }

        std::string last_key = "";
        idx_t now_file_id = compaction.file_id;

        while (!q.empty()) {
            TupleMessage<std::string> now_message = q.top();
            q.pop();

            if (now_message.offset < key_num[now_message.file_idx] - 1) {
                q.emplace(keys[now_message.file_idx][now_message.offset + 1],
                          now_message.offset + 1, now_message.file_idx);
            }

            if (now_message.key == last_key) {
                continue;
            }
            last_key = now_message.key;

            order_key_buf[key_buf_idx] = now_message.key;
            for (idx_t i = 0; i < col_num; i++) {
                idx_t tmp_val = vals[now_message.file_idx][i][now_message.offset];
                val_buf[i][key_buf_idx] = std::make_pair(now_message.file_idx, tmp_val);
                if (!remap[i][now_message.file_idx][tmp_val]) {
                    remap[i][now_message.file_idx][tmp_val] = 1;
                    //auto nowstr = (*DictList[now_message.file_idx])[i].getString(val_buf[i][key_buf_idx].second);
                    auto nowstr = (DictList[now_message.file_idx])->at(i).getString(val_buf[i][key_buf_idx].second);
                    auto it = s[i].lower_bound(nowstr);
                    if (it != s[i].end() && it->first == nowstr) {
                        it->second.emplace_back(now_message.file_idx, tmp_val);
                    } else {
                        std::vector<std::pair<int, idx_t> > tmp;
                        tmp.emplace_back(now_message.file_idx, tmp_val);
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
                    for (auto &entry: s[i]) {
                        dict.emplace_back(entry.first);
                        for (auto p: entry.second) {
                            remap[i][p.first][p.second] = nowidx;
                        }
                        nowidx++;
                    }
                    temp_file_metadata->dictionary.emplace_back(dict);
                }
                //map new index from new dictionary
                for (idx_t i = 0; i < col_num; i++) {
                    for (int j = 0; j < key_buf_idx; j++) {
                        auto p = val_buf[i][j];
                        real_val_buf[i][j] = remap[i][p.first][p.second];
                    }
                }

                //write current buffer to file
                rel_builder->ArrangeRelFileInfo(order_key_buf, key_buf_idx, db->options->KEY_SIZE, col_num,
                                                real_val_buf);
                temp_file_metadata->key_min = std::string(order_key_buf[0].c_str());
                temp_file_metadata->key_max = std::string(last_key);
                temp_file_metadata->key_num = key_buf_idx;
                temp_file_metadata->col_num = col_num;
                temp_file_metadata->block_count = rel_builder->GetBlockCount();
                temp_file_metadata->block_filter_size = rel_builder->GetBlockFilterSize();
                temp_file_metadata->block_meta_begin_pos = rel_builder->GetBlockMetaBeginPos();
                temp_file_metadata->block_func_num = rel_builder->GetBlockFuncNum();
                temp_file_metadata->bloom_filter = BloomFilter(key_buf_idx, db->options->FALSE_POSITIVE);
                for (int i = 0; i < key_buf_idx; i++) {
                    temp_file_metadata->bloom_filter.insert(order_key_buf[i]);
                }
                edit->EditFileList.push_back(temp_file_metadata);

                //reset buffer information
                key_buf_idx = 0;
                for (idx_t i = 0; i < col_num; i++) {
                    for (int j = 0; j < file_num; j++) {
                        remap[i][j].clear();
                        remap[i][j].resize(max_val[j] + 5);
                    }
                }
                for (idx_t i = 0; i < col_num; i++) {
                    s[i].clear();
                }
                if (!q.empty()) {
                    //open new file
                    temp_file_metadata = new RelFileMetaData<std::string>(0, compaction.target_level,
                                                                          compaction.vertex_id_b,
                                                                          ++now_file_id, "", q.top().key, "", 0, 0);
                    std::string file_name = temp_file_metadata->file_name;
                    fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/"
                                                      + file_name);
                    delete rel_builder;
                    rel_builder = new RelFileBuilder<std::string>(fw, db->options);
                }
            }
        }

        if (key_buf_idx) {
            //if it has deletion, an extra flush is needed;
            //flush buf to a new file
            //build new dict
            int nowidx = 0;
            for (idx_t i = 0; i < col_num; i++) {
                std::vector<std::string> dict;
                for (auto &entry: s[i]) {
                    dict.emplace_back(entry.first);
                    for (auto p: entry.second) {
                        remap[i][p.first][p.second] = nowidx;
                    }
                    nowidx++;
                }
                temp_file_metadata->dictionary.emplace_back(dict);
            }
            //map new index from new dictionary
            for (idx_t i = 0; i < col_num; i++) {
                for (int j = 0; j < key_buf_idx; j++) {
                    auto p = val_buf[i][j];
                    real_val_buf[i][j] = remap[i][p.first][p.second];
                }
            }

            //write current buffer to file
            rel_builder->ArrangeRelFileInfo(order_key_buf, key_buf_idx, db->options->KEY_SIZE, col_num,
                                            real_val_buf);
            temp_file_metadata->key_min = std::string(order_key_buf[0].c_str());
            temp_file_metadata->key_max = std::string(last_key);
            temp_file_metadata->key_num = key_buf_idx;
            temp_file_metadata->col_num = col_num;
            temp_file_metadata->block_count = rel_builder->GetBlockCount();
            temp_file_metadata->block_filter_size = rel_builder->GetBlockFilterSize();
            temp_file_metadata->block_meta_begin_pos = rel_builder->GetBlockMetaBeginPos();
            temp_file_metadata->block_func_num = rel_builder->GetBlockFuncNum();
            temp_file_metadata->bloom_filter = BloomFilter(key_buf_idx, db->options->FALSE_POSITIVE);
            for (int i = 0; i < key_buf_idx; i++) {
                temp_file_metadata->bloom_filter.insert(order_key_buf[i]);
            }
            edit->EditFileList.push_back(temp_file_metadata);
        }

        for (auto &file: compaction.file_list) {
            edit->EditFileList.push_back(file);
            edit->EditFileList.back()->deletion = true;
        }

        for (int i = 0; i < file_num; i++) {
            delete[] keys[i];
            for (idx_t j = 0; j < col_num; j++) {
                free(vals[i][j]);
            }
        }
        if (rel_builder) delete rel_builder;
        if (order_key_buf) delete[] order_key_buf;
        for (idx_t i = 0; i < col_num; i++) {
            if (real_val_buf[i]) free(real_val_buf[i]);
        }
        DictList.clear();
        return edit;
    }
}
