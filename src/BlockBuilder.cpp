#include "BACH/sstable/BlockBuilder.h"
#include "BACH/db/DB.h"

namespace BACH {
    BlockBuilder::BlockBuilder(std::shared_ptr<FileWriter>_filewriter, std::shared_ptr<Options>_options) :
        writer(_filewriter),
        options(_options){}
    
    void BlockBuilder::SetKeyRange(Key_t key_min, Key_t key_max) {
        this->key_min = key_min;
        this->key_max = key_max;
    }
    
    void BlockBuilder::SetKey(Key_t* keys, idx_t key_num = 0, size_t key_size = 0) {
        if(key_size) {
            writer->append(static_cast<char*>keys, key_size);
        } else if (key_num) {
            std::string temp_data;
            for(int i = 0; i < key_num; i++) {
                util::PutFixed(temp_data, keys[i])
            }
            writer->append(temp_data.data(), temp_data.size());
        } else {
            std::cerr << "No implement: Set keys without num or size" << std::endl;
            assert(0);
        }
    }

    void BlockBuilder::SetValueIdx(idx_t* vals, idx_t val_num = 0) {
        writer->append(static_cast<char*>vals, (int32_t)val_num * sizeof(idx_t));
    }
    
    void ArrangeBlockInfo(BloomFilter &filter, Key_t *key, idx_t key_num, idx_t col_num) {
        for(int i = 0; i < key_num; i++) {
            filter.insert(key[i]);
        }
        std::string metadata;
        util::PutFixed(metadata, key_num);
        util::PutFixed(metadata, col_num);
        writer->append(metadata.data(), metadata.size());
        writer->flush();
    }
}