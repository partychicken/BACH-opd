#include "BACH/memory/RowGroup.h"
#include "BACH/db/DB.h"

namespace BACH {
    template<typename Key_t>
    RowGroup<Key_t>::RowGroup(DB* _db, idx_t key_num, idx_t col_num, RelFileMetaData<Key_t>* _file)
            : db(_db), key_num(key_num), col_num(col_num), file(_file) {
        scan_pos = static_cast<idx_t*>(malloc(col_num * sizeof(idx_t)));
        memset(scan_pos, 0, col_num * sizeof(idx_t));
        keys = static_cast<Key_t*>(malloc(key_num * sizeof(Key_t)));
        cols.resize(col_num);
        for(int i = 0; i < col_num; i++) {
            cols[i] = static_cast<idx_t*>(malloc(key_num * sizeof(idx_t)));
        }
        auto reader = db->ReaderCaches->find(file);
        parser = new RelFileParser<Key_t>(reader, db->options, file->file_size);
    }

    template<typename Key_t>
    template<typename Func>
    void RowGroup<Key_t>::ApplyRangeFilter(idx_t col_id, Func* left_bound, Func* right_bound){
        Vector res;
        while(Scan(col_id, res)) {

        }
    }

}