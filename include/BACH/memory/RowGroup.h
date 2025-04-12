#pragma once
#include <memory>
#include "BACH/sstable/RelFileParser.h"
#include "BACH/utils/types.h"
#include "BACH/sstable/FileMetaData.h"
#include "BACH/common/vector.h"
#include "BACH/vectorized_execution/operator.h"

namespace BACH {
    class DB;

    //RowGroup holds the real memory when doing AP ops
    //needs to malloc & free the memory manually
    template<typename Key_t>
    class RowGroup {
    public:
        RowGroup(DB* _db, idx_t key_num, idx_t col_num, RelFileMetaData<Key_t>* _file);

        ~RowGroup() {
            if(keys != nullptr) free(keys);
            for(auto x : cols) {
                if(x != nullptr) free(x);
            }
        }

        void GetKeyData() {
            parser->GetKeyCol(keys, key_num);
        }

        void GetColData(idx_t col_id) {
            parser->GetValCol(cols[col_id], key_num, col_id);
        }

        bool Scan(idx_t col_id, Vector &result) {
            idx_t scan_num = key_num - scan_pos[col_id];
            if(!scan_num) return false;
            result = Vector(make_share(file->dictionary[col_id]));
            if(scan_num >= STANDARD_VECTOR_SIZE) {
                result.SetData(cols[col_id] + scan_pos[col_id], STANDARD_VECTOR_SIZE);
                scan_pos[col_id] += STANDARD_VECTOR_SIZE;
            } else {
                result.SetData(cols[col_id] + scan_pos[col_id], scan_num);
                scan_pos[col_id] += scan_num;
            }
            return true;
        }

        template<typename Func>
        void ApplyRangeFilter(idx_t col_id, Func* left_bound, Func* right_bound);

        void Materialize();
    private:
        DB* db;
        idx_t key_num, col_num;
        RelFileMetaData<Key_t>* file;
        idx_t* scan_pos;
        Key_t* keys;
        std::vector<idx_t*> cols;
        RelFileParser<Key_t>* parser;

        friend class DB;
    };
}