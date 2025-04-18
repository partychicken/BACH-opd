#pragma once
#include <memory>
#include "BACH/sstable/RelFileParser.h"
#include "BACH/utils/types.h"
#include "BACH/sstable/FileMetaData.h"
#include "BACH/common/vector.h"
#include "BACH/memory/AnswerMerger.h"
#include "BACH/vectorized_execution/operator.h"

namespace BACH {
    class DB;

    //RowGroup holds the real memory when doing AP ops
    //needs to malloc & free the memory manually
    template<typename Key_t>
    class RowGroup {
    public:
        RowGroup(DB* _db,  RelFileMetaData<Key_t>* _file);

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

        void GetAllColData() {
            for (int i = 0; i < col_num; i++) {
                GetColData(i);
            }
        }

        bool Scan(idx_t col_id, Vector &result);
        void Materialize(Vector &result, AnswerMerger &am);

        template<typename Func>
        void ApplyRangeFilter(idx_t col_id, Func* left_bound, Func* right_bound, AnswerMerger &am);

        void MaterializeAll(AnswerMerger &am);

    private:
        DB* db;
        idx_t key_num, col_num;
        RelFileMetaData<Key_t>* file;
        idx_t* scan_pos;
        Key_t* keys;
        std::vector<idx_t*> cols;
        RelFileParser<Key_t>* parser;

        OrderedDictionary* dict(idx_t col_id) {
            return file->dictionary[col_id];
        }

        friend class DB;
    };
}