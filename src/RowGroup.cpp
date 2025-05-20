#include "BACH/memory/RowGroup.h"
#include "BACH/db/DB.h"

namespace BACH {

    RowGroup::RowGroup(DB *_db,  RelFileMetaData<std::string> *_file)
        : db(_db), file(_file) {
        key_num = file->key_num;
        col_num = file->col_num;
        scan_pos = static_cast<idx_t *>(malloc(col_num * sizeof(idx_t)));
        memset(scan_pos, 0, col_num * sizeof(idx_t));
        //keys = static_cast<std::string *>(malloc(key_num * sizeof(std::string)));
        keys = new std::string[key_num];
        cols.resize(col_num);
        for (idx_t i = 0; i < col_num; i++) {
            cols[i] = static_cast<idx_t *>(malloc(key_num * sizeof(idx_t)));
        }
        auto reader = db->ReaderCaches->find(file);
        parser = new RelFileParser<std::string>(reader, db->options, file->file_size, file);
    }


    bool RowGroup::Scan(idx_t col_id, Vector &result) {
        idx_t scan_num = key_num - scan_pos[col_id];
        if (!scan_num) return false;
        result = Vector( std::make_shared<OrderedDictionary>(file->dictionary[col_id]));
        if (scan_num >= STANDARD_VECTOR_SIZE) {
            result.SetData(cols[col_id] + scan_pos[col_id], STANDARD_VECTOR_SIZE, scan_pos[col_id]);
            scan_pos[col_id] += STANDARD_VECTOR_SIZE;
        } else {
            result.SetData(cols[col_id] + scan_pos[col_id], scan_num, scan_pos[col_id]);
            scan_pos[col_id] += scan_num;
        }
        return true;
    }


    void RowGroup::Materialize(Vector &result, AnswerMerger &am) {
        sul::dynamic_bitset<> bitmap;
        result.GetBitmap(bitmap);
        idx_t offset = result.GetOffset();
        idx_t cnt = result.GetCount();
        for (idx_t i = 0; i < cnt; i++) {
            if (bitmap[i]) {
                Tuple tmp;
                tmp.col_num = col_num;
                tmp.row.push_back(keys[offset + i]);
                for (idx_t j = 0; j < col_num - 1; j++) {
                    tmp.row.push_back(dict(j)->getString(cols[j][offset + i]));
                }
                am.insert_answer(keys[offset + i], std::move(tmp), 0);
				//std::cout << "\nMaterialize: " << keys[offset + i]  << std::endl;
            }
        }
    }


    //template<typename Func>
    //void RowGroup::ApplyRangeFilter(idx_t col_id, Func *left_bound, Func *right_bound, AnswerMerger &am) {
    //    Vector res;
    //    while (Scan(col_id, res)) {
    //        RangeFilter(res, left_bound, right_bound);
    //        Materialize(res, am);
    //    }
    //}


    void RowGroup::MaterializeAll(AnswerMerger &am) {
        Vector res;
        while (Scan(0, res)) {
            Materialize(res, am);
        }
    }

}
