#include <memory>
#include "BACH/sstable/RelFileParser.h"
#include "BACH/utils/types.h"
#include "BACH/sstable/FileMetaData.h"

namespace BACH {

    //RowGroup holds the real memory when doing AP ops
    //needs to malloc & free the memory manually
    template<typename Key_t>
    class RowGroup {
    public:
        RowGroup(){}
        RowGroup(idx_t key_num, idx_t col_num, RelFileMetaData<Key_t>* _file)
            : key_num(key_num), col_num(col_num), file(_file) {
            keys = static_cast<Key_t*>(malloc(key_num * sizeof(Key_t)));
            cols.resize(col_num);
            for(int i = 0; i < col_num; i++) {
                cols[i] = static_cast<idx_t*>(malloc(key_num * sizeof(idx_t)));
            }
        }

        ~RowGroup() {
            if(keys != nullptr) free(keys);
            for(auto x : cols) {
                if(x != nullptr) free(x);
            }
        }

        void Scan() {
        }
    private:
        idx_t key_num, col_num;

        RelFileMetaData<Key_t>* file;
        Key_t* keys;
        std::vector<idx_t*> cols;
    };
}