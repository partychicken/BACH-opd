#include "BACH/sstable/RelFileManager.h"
#include "BACH/db/DB.h"

namespace BACH {
    template<typename Key_t>
    RelFileManager<Key_t>::RelFileManager(DB *_db) : db(_db) {
    }

    template<typename Key_t>
    void RelFileManager<Key_t>::AddCompaction(Compaction &compaction) {
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

        TupleMessage(Key_t key, idx_t offset, int file_idx) : key(key), offset(offset), file_idx(file_idx) {
        }
    };

    struct DictMappingEntry {
        std::string key;
        std::vector<std::pair<int, int> > us; //<file_id, origin_index>
        bool operator <(const DictMappingEntry &other) const {
            return key < other.key;
        }

        bool operator ==(const DictMappingEntry &other) const {
            return key == other.key;
        }
    };


    //把多个文件归并后生成一个新的文件，然后生成新的Version并将current_version指向这个新的version，然后旧的version如果ref为0就删除这个version并将这个version对应的文件的ref减1，如果文件ref为0则物理删除
    template<typename Key_t>
    VersionEdit *RelFileManager<Key_t>::MergeRelFile(Compaction &compaction) {
        std::vector<RelFileParser<Key_t> > parsers;
        std::vector<int16_t> file_ids;
        for (auto &file: compaction.file_list) {
            auto reader = db->ReaderCaches->find(file);
            parsers.emplace_back(reader, db->options, file->file_size);
            DictList = static_cast<RelFileMetaData<Key_t>>(file).dictionary;
            if (file->level == compaction.target_level)
                file_ids.push_back(-db->options->FILE_MERGE_NUM - 10 + file->file_id);
            else
                file_ids.push_back(file->file_id + 1);
        }

        int file_num = file_ids.size();
        idx_t col_num = parsers.begin()->GetColumnNum(); // not check the consistency among files

        idx_t key_num_tot = 0;
        std::priority_queue<TupleMessage<Key_t>, std::vector<TupleMessage<Key_t> >,
            std::greater<TupleMessage<Key_t> > > q;
        Key_t new_file_key_min = static_cast<RelCompaction<Key_t>>(compaction).key_min;

        Key_t *keys[file_num];
        idx_t *vals[file_num][col_num];
        int key_num[file_num], now_idx[file_num];
        int key_tot_num = 0, key_done = 0;
        memset(now_idx, 0, sizeof(now_idx));
        for (size_t i = 0; i < parsers.size(); i++) {
            keys[i] = static_cast<Key_t *>(malloc(sizeof(key_t) * parsers[i].key_num));
            int check_size = 0;
            parsers[i].GetKeyCol(keys[i], check_size);
            if (check_size != parsers[i].key_num) {
                return nullptr;
            }
            key_tot_num += (key_num[i] = check_size);

            for (int j = 0; j < col_num; j++) {
                vals[i][j] = static_cast<idx_t *>(malloc(sizeof(idx_t) * parsers[i].key_num));
                parsers[i].GetValCol(vals[i][j], check_size, j);
            }
            q.push(TupleMessage<Key_t>(keys[i][0], 0, i));
        }


        auto temp_file_metadata = new RelFileMetaData<Key_t>(0, compaction.target_level, compaction.vertex_id_b,
                                                             compaction.file_id, "rel", new_file_key_min);
        std::string file_name = temp_file_metadata->file_name;
        auto fw = std::make_shared<FileWriter>(db->options->STORAGE_DIR + "/"
                                               + file_name);
        auto rel_builder = new RelFileBuilder(fw, db->options);

        Key_t *order_key_buf = static_cast<Key_t *>(malloc(sizeof(Key_t) * (key_tot_num / file_num + 5)));
        int key_buf_idx = 0;
        std::pair<idx_t, idx_t> *val_buf[col_num];
        for (int i = 0; i < col_num; i++) {
            val_buf[i] = static_cast<std::pair<idx_t, idx_t> *>(malloc(sizeof(std::pair<idx_t, idx_t>) * (key_tot_num / file_num + 5)));
        }

        idx_t *real_val_buf[col_num];
        for (int i = 0; i < col_num; i++) {
            real_val_buf[i] = static_cast<idx_t *>(malloc(sizeof(idx_t) * (key_tot_num / file_num + 5)));
        }

        VersionEdit *edit = new VersionEdit();

        std::set<DictMappingEntry> s[col_num];
        int remap[col_num][file_num][key_num]; //third dimension is larger than the necessary's, needing optimized
        // in the first stage, remap denotes whether the index exists in set;
        // in the second stage, remap denotes the new index that the old one should be mapped to
        memset(remap, 0, sizeof(remap));

        Key_t last_key = 0;

        while (!q.empty()) {
            TupleMessage<Key_t> now_message = q.top();
            q.pop();

            if (now_message.offset < key_num[now_message.file_idx]) {
                q.push(TupleMessage<Key_t>(keys[now_message.file_idx][now_message.offset + 1],
                                           now_message.offset + 1, now_message.file_idx));
            }

            if (now_message.key == last_key) {
                continue;
            }

            order_key_buf[key_buf_idx] = now_message.key;
            for (int i = 0; i < col_num; i++) {
                val_buf[i][key_buf_idx] = make_pair(now_message.file_idx,
                                                    vals[now_message.file_idx][i][now_message.offset]);
                if (!remap[i][now_message.file_idx][now_message.offset]) {
                    remap[i][now_message.file_idx][now_message.offset] = 1;
                    auto nowstr = DictList[i]->getString(val_buf[i][key_buf_idx]);
                    auto it = s[i].lower_bound(DictMappingEntry(nowstr, 0));
                    if (it != s[i].end() && it->key == nowstr) {
                        it->us.push_back(std::make_pair(now_message.file_idx, now_message.offset));
                    } else {
                        std::vector<std::pair<int, int> > tmp;
                        tmp.push_back(std::make_pair(now_message.file_idx, now_message.offset));
                        s[i].insert(DictMappingEntry(nowstr, tmp));
                    }
                }
            }
            key_buf_idx++;

            if (key_buf_idx * file_num >= key_tot_num) {
                //flush buf to a new file
                //build new dict
                int nowidx = 0;
                for (int i = 0; i < col_num; i++) {
                    std::vector<std::string> dict;
                    for (auto entry: s[i]) {
                        dict.emplace_back(entry.key);
                        for (auto p: entry.us) {
                            remap[i][p.first][p.second] = nowidx;
                        }
                    }
                    temp_file_metadata->dictionary.push_back(OrderedDictionary(dict));
                }
                //map new index from new dictionary
                for (int i = 0; i < col_num; i++) {
                    for (int j = 0; j < key_buf_idx; j++) {
                        auto p = val_buf[i][j];
                        real_val_buf[i][j] = remap[i][p.first][p.second];
                    }
                }
                rel_builder->ArrangeRelFileInfo(order_key_buf, key_buf_idx, sizeof(Key_t), col_num, real_val_buf);
                edit->EditFileList.push_back(std::move(*temp_file_metadata));

                key_buf_idx = 0;
                memset(remap, 0, sizeof(remap));
                s.clear();
            }
        }

        for (auto &file: compaction.file_list) {
            edit->EditFileList.push_back(std::move(*file));
            edit->EditFileList.back().deletion = true;
        }
        return edit;
    }
}
