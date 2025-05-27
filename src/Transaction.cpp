#include "BACH/db/Transaction.h"

#include <BACH/common/ScanIter.h>

namespace BACH {
    Transaction::Transaction(time_t _write_epoch, time_t _read_epoch,
                             DB *db, Version *_version, time_t pos) : write_epoch(_write_epoch),
                                                                      read_epoch(_read_epoch), db(db), time_pos(pos),
                                                                      version(_version), rel_version(nullptr) {
    }

    Transaction::Transaction(time_t _write_epoch, time_t _read_epoch,
                             DB *db, RelVersion *_version, time_t pos) : write_epoch(_write_epoch),
                                                                         read_epoch(_read_epoch), db(db), time_pos(pos),
                                                                         version(nullptr), rel_version(_version) {
    }

    Transaction::Transaction(Transaction &&txn) : write_epoch(txn.write_epoch), read_epoch(txn.read_epoch),
                                                  db(txn.db), time_pos(txn.time_pos), version(txn.version),
                                                  rel_version(txn.rel_version) {
        txn.valid = false;
    }

    Transaction::~Transaction() {
        if (valid) {
            if (version) version->DecRef();
            if (rel_version) rel_version->DecRef();
            if (write_epoch != MAXTIME) {
                db->write_epoch_table.erase(time_pos);
                if (version) db->ProgressReadVersion();

                if (rel_version) db->ProgressReadRelVersion();
            }
#ifdef RUN_PROFILER
            db->db_profiler.AddThreadProfiler(profiler);
#endif
        }
    }

    vertex_t Transaction::AddVertex(label_t label) {
        if (write_epoch == MAXTIME) {
            return MAXVERTEX;
        }
        return db->Memtable->AddVertex(label);
    }

    void Transaction::PutVertex(label_t label, vertex_t vertex_id, std::string_view property) {
        if (write_epoch == MAXTIME) {
            return;
        }
        db->Memtable->PutVertex(label, vertex_id, property);
    }

    std::shared_ptr<std::string> Transaction::GetVertex(
        vertex_t vertex, label_t label) {
        return db->Memtable->GetVertex(vertex, label, read_epoch);
    }

    void Transaction::DelVertex(vertex_t vertex, label_t label) {
        if (write_epoch == MAXTIME) {
            return;
        }
        db->Memtable->DelVertex(vertex, label, write_epoch);
    }

    vertex_t Transaction::GetVertexNum(label_t label) {
        return db->Memtable->GetVertexNum(label);
    }

    void Transaction::PutEdge(vertex_t src, vertex_t dst, label_t label,
                              edge_property_t property, bool delete_old) {
        if (write_epoch == MAXTIME) {
            return;
        }
        //if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
        //{
        //	std::cout<<"add edge from a deleted vertex!\n";
        //}
        db->Memtable->PutEdge(src, dst, label, property, write_epoch);
    }

    void Transaction::DelEdge(vertex_t src, vertex_t dst, label_t label) {
        if (write_epoch == MAXTIME) {
            return;
        }
        //if (db->Memtable->GetVertexDelTime(label, src) < now_epoch)
        //{
        //	std::cout << "delete edge from a deleted vertex!\n";
        //}
        db->Memtable->DelEdge(src, dst, label, write_epoch);
    }

    edge_property_t Transaction::GetEdge(
        vertex_t src, vertex_t dst, label_t label) {
        edge_property_t x = db->Memtable->GetEdge(src, dst, label, read_epoch);
        if (!std::isnan(x))
            return x;
        VersionIterator iter(version, label, src);
        while (!iter.End()) {
            if (src - iter.GetFile()->vertex_id_b < iter.GetFile()->filter->size())
                if ((*iter.GetFile()->filter)[src - iter.GetFile()->vertex_id_b]) {
                    SSTableParser parser(label, db->ReaderCaches->find(iter.GetFile()), db->options);
                    auto found = parser.GetEdge(src, dst);
                    if (!std::isnan(found))
                        return found;
                }
            iter.next();
        }
        return TOMBSTONE;
    }

    std::shared_ptr<std::vector<std::pair<vertex_t, edge_property_t> > >
    Transaction::GetEdges(vertex_t src, label_t label,
                          const std::function<bool(edge_property_t &)> &func) {
        std::shared_ptr<std::vector<std::pair<
            vertex_t, edge_property_t> > > answer_temp[3];
        vertex_t c = 0;
        for (size_t i = 0; i < 3; i++)
            answer_temp[i] = std::make_shared<std::vector<
                std::pair<vertex_t, edge_property_t> > >();
        db->Memtable->GetEdges(src, label, read_epoch, answer_temp, c, func);
        VersionIterator iter(version, label, src);
        while (!iter.End()) {
            if (src - iter.GetFile()->vertex_id_b < iter.GetFile()->filter->size())
                if ((*iter.GetFile()->filter)[src - iter.GetFile()->vertex_id_b]) {
                    SSTableParser parser(label, db->ReaderCaches->find(iter.GetFile()), db->options);
                    parser.GetEdges(src, answer_temp[(c + 1) % 3], func);
                    db->Memtable->merge_answer(answer_temp, c);
                }
            iter.next();
        }
        return answer_temp[c];
    }

    void Transaction::EdgeLabelScan(
        label_t label, const std::function<void(vertex_t &, vertex_t &, edge_property_t &)> &func) {
        db->Memtable->EdgeLabelScan(label, func);
        if (version->FileIndex.size() <= label)
            return;
        for (auto &i: version->FileIndex[label])
            for (auto &j: i) {
                SSTableParser parser(label, db->ReaderCaches->find(j), db->options);
                if (!parser.GetFirstEdge())
                    continue;
                do {
                    auto src = parser.GetNowEdgeSrc();
                    auto dst = parser.GetNowEdgeDst();
                    auto prop = parser.GetNowEdgeProp();
                    func(src, dst, prop);
                } while (parser.GetNextEdge());
            }
    }


    void Transaction::PutTuple(Tuple tuple, tp_key key) {
        if (write_epoch == MAXTIME) {
            return;
        }
        db->check_write_stall();
        // START_OPERATOR_PROFILER();
        db->RowMemtable->PutTuple(tuple, key, write_epoch);
        // END_OPERATOR_PROFILER("PutTuple");
    }

    void Transaction::DelTuple(tp_key key) {
        if (write_epoch == MAXTIME) {
            return;
        }
        db->check_write_stall();
        // START_OPERATOR_PROFILER();
        Tuple x;
        x.row.push_back(key);
        db->RowMemtable->PutTuple(x, key, write_epoch, true);
        // END_OPERATOR_PROFILER("DelTuple");
    }

    Tuple Transaction::GetTuple(tp_key key) {
        // START_OPERATOR_PROFILER();
        Tuple x = db->RowMemtable->GetTuple(key, read_epoch, rel_version);
        // if (!std::isnan(x.property)) {
        //     return x;
        // }
        if (!x.row.empty()) {
            // END_OPERATOR_PROFILER("GetTuple");
            return x;
        }
        // key.resize(db->options->KEY_SIZE);
        RelVersionIterator iter(rel_version, key, key);
        while (!iter.End()) {
            if (key >= reinterpret_cast<RelFileMetaData<std::string> *>(iter.GetFile())->key_min && key <=
                reinterpret_cast<RelFileMetaData<std::string> *>(iter.GetFile())->key_max) {
                // ԭ���ж�����Ϊ(*iter.GetFile()->filter)[src - iter.GetFile()->vertex_id_b]
                if (iter.GetFile()->bloom_filter.exists(key)) {

                    RelFileParser<std::string>* parser = iter.GetFile()->parser;
                    Tuple found = parser->GetTuple(key);
                    if (!found.row.empty()) {
                        for (size_t i = 1; i < found.row.size(); i++) {
                            idx_t col_id = *((idx_t *) found.row[i].data());
                            found.row[i] = static_cast<RelFileMetaData<std::string> *>(iter.GetFile())->dictionary[
                                i - 1].
                            getString(col_id);
                        }
                        // END_OPERATOR_PROFILER("GetTuple");
                        return found;
                    }
                }
            }
            iter.next();
        }
        // END_OPERATOR_PROFILER("GetTuple");
        return Tuple();
    }

    //template<typename Func>
    //void Transaction::GetTuplesFromRange(idx_t col_id, Func *left_bound, Func *right_bound) {
    //    AnswerMerger am;
    //    // add in-memory data into am here;
    //    auto vf = CreateValueFilterFunction<Func>(col_id, *left_bound, *right_bound);
    //    db->RowMemtable->FilterByValueRange(write_epoch, vf, am);

    //    auto files = rel_version->FileIndex;
    //    for (auto cur_level: files) {
    //        for (auto cur_file: cur_level) {
    //            auto reader = db->ReaderCaches->find(cur_file);
    //            auto parser = RelFileParser<std::string>(reader, db->options, cur_file->file_size);
    //            auto DictList = static_cast<RelFileMetaData<std::string> *>(cur_file)->dictionary;
    //            RowGroup cur_row_group(db, static_cast<RelFileMetaData<std::string> *>(cur_file));
    //            cur_row_group.GetKeyData();
    //            cur_row_group.GetAllColData();
    //            cur_row_group.ApplyRangeFilter(col_id, left_bound, right_bound, am);
    //        }
    //    }
    //}

    void Transaction::ScanTuples() {
        START_OPERATOR_PROFILER();

        AnswerMerger am;

        auto files = rel_version->FileIndex;
        for (auto cur_level: files) {
            for (auto cur_file: cur_level) {
                auto reader = db->ReaderCaches->find(cur_file);
                auto parser = RelFileParser<std::string>(reader, db->options, cur_file->file_size, static_cast<RelFileMetaData<std::string> *>(cur_file));
                auto DictList = reinterpret_cast<RelFileMetaData<std::string> *>(cur_file)->dictionary;
                RowGroup cur_row_group(db, reinterpret_cast<RelFileMetaData<std::string> *>(cur_file));
                cur_row_group.GetKeyData();
                cur_row_group.GetAllColData();
                cur_row_group.MaterializeAll(am);
            }
        }
        END_OPERATOR_PROFILER("ScanTuples");
    }

    std::vector<Tuple> Transaction::ScanKTuples(idx_t k, std::string key) {
        // START_OPERATOR_PROFILER();

        std::map<std::string, Tuple> am;
        db->RowMemtable->GetKTuple(k, key, read_epoch, am, rel_version);

        std::string KEY_MAX;
        for (unsigned int i = 0; i < db->options->KEY_SIZE; i++) KEY_MAX += ((char) 127);
        RelVersionIterator iter(rel_version, key, KEY_MAX);
        while (!iter.End()) {
            auto cur_file = iter.GetFile();
            auto reader = db->ReaderCaches->find(cur_file);
            auto parser = RelFileParser<std::string>(reader, db->options, cur_file->file_size, static_cast<RelFileMetaData<std::string> *>(cur_file));
            std::vector<Tuple> res = parser.GetKTuple(k, key);
            for (auto x: res) {
                if (am.contains(x.GetKey())) continue;
                if (!x.row.empty()) {
                    for (size_t i = 1; i < x.row.size(); i++) {
                        idx_t col_id = *((idx_t *) x.row[i].data());
                        x.row[i] = static_cast<RelFileMetaData<std::string> *>(iter.GetFile())->dictionary[i - 1].
                        getString(col_id);
                    }
                    am.emplace(x.GetKey(), std::move(x));
                }
            }
            iter.next();
        }
        std::vector<Tuple> res;
        for (auto &x : am) {
            res.emplace_back(x.second);
            if (res.size() >= k) break;
        }
        // END_OPERATOR_PROFILER("ScanKTuples");
        return res;
    }

    ScanIter Transaction::GetIter(std::string key) {
        return ScanIter(rel_version, db->RowMemtable->GetCurrentTable(), key);
    }
}
