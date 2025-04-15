#include "BACH/memory/RowGroup.h"
#include "BACH/db/DB.h"

namespace BACH {
    template<typename Key_t>
    RowGroup<Key_t>::RowGroup(DB *_db, idx_t key_num, idx_t col_num, RelFileMetaData<Key_t> *_file)
        : db(_db), key_num(key_num), col_num(col_num), file(_file) {
        scan_pos = static_cast<idx_t *>(malloc(col_num * sizeof(idx_t)));
        memset(scan_pos, 0, col_num * sizeof(idx_t));
        keys = static_cast<Key_t *>(malloc(key_num * sizeof(Key_t)));
        cols.resize(col_num);
        for (int i = 0; i < col_num; i++) {
            cols[i] = static_cast<idx_t *>(malloc(key_num * sizeof(idx_t)));
        }
        auto reader = db->ReaderCaches->find(file);
        parser = new RelFileParser<Key_t>(reader, db->options, file->file_size);
    }

    template<typename Key_t>
    bool RowGroup<Key_t>::Scan(idx_t col_id, Vector &result) {
        idx_t scan_num = key_num - scan_pos[col_id];
        if (!scan_num) return false;
        result = Vector(make_share(file->dictionary[col_id]));
        if (scan_num >= STANDARD_VECTOR_SIZE) {
            result.SetData(cols[col_id] + scan_pos[col_id], STANDARD_VECTOR_SIZE, scan_pos[col_id]);
            scan_pos[col_id] += STANDARD_VECTOR_SIZE;
        } else {
            result.SetData(cols[col_id] + scan_pos[col_id], scan_num, scan_pos[col_id]);
            scan_pos[col_id] += scan_num;
        }
        return true;
    }

    template<typename Key_t>
    void RowGroup<Key_t>::Materialize(Vector &result, AnswerMerger &am) {
        sul::dynamic_bitset<> bitmap;
        result.GetBitmap(bitmap);
        idx_t offset = result.GetOffset();
        idx_t cnt = result.GetCount();
        for (idx_t i = 0; i < cnt; i++) {
            if (bitmap[i]) {
                Tuple tmp;
                tmp.col_num = col_num;
                tmp.row.push_back(keys[offset + i]);
                for (int j = 0; j < col_num; j++) {
                    tmp.row.push_back(dict(j)->getString(cols[j][offset + i]));
                }
                am.insert_answer(keys[offset + i], tmp, 0);
            }
        }
    }

    template<typename Key_t>
    template<typename Func>
    void RowGroup<Key_t>::ApplyRangeFilter(idx_t col_id, Func *left_bound, Func *right_bound, AnswerMerger &am) {
        Vector res;
        while (Scan(col_id, res)) {
            RangeFilter(res, left_bound, right_bound);
            Materialize(res, am);
        }
    }
}
