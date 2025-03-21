#include "BACH/sstable/BlockBuilder.h"
#include "BACH/db/DB.h"

namespace BACH {
    template <typename Key_t>
    BlockBuilder<Key_t>::BlockBuilder(std::shared_ptr<FileWriter>_filewriter, std::shared_ptr<Options>_options) :
        writer(_filewriter),
        options(_options){}

    template <typename Key_t>
    void BlockBuilder<Key_t>::SetKeyRange(const Key_t &key_min,const Key_t &key_max) {
        this->key_min = key_min;
        this->key_max = key_max;
    }

    template <typename Key_t>
    void BlockBuilder<Key_t>::SetKey(Key_t* keys, idx_t key_num, size_t key_size) {
        if(key_size) {
            writer->append(reinterpret_cast<char*>(keys), key_size);
        } else if (key_num) {
            std::string temp_data;
            for(int i = 0; i < key_num; i++) {
                util::PutFixed(temp_data, keys[i]);
            }
            writer->append(temp_data.data(), temp_data.size());
        } else {
            std::cerr << "No implement: Set keys without num or size" << std::endl;
            assert(0);
        }
    }

    template <typename Key_t>
    size_t BlockBuilder<Key_t>::SetValueIdx(idx_t* vals, idx_t val_num) {
        //vals can be compressed here, return the bitpacked size
        writer->append(reinterpret_cast<char*>(vals), (int32_t)val_num * sizeof(idx_t));
        return sizeof(idx_t);
    }

    template <typename Key_t>
    void BlockBuilder<Key_t>::ArrangeBlockInfo(BloomFilter &filter, Key_t *key, idx_t key_num, idx_t col_num,
                                               size_t key_size, size_t* col_size) {
        Key_t key_max = key[0], key_min = key[0];
        for(int i = 0; i < key_num; i++) {
            filter.insert(key[i]);
            key_max = max(key_max, key[i]);
            key_min = min(key_min, key[i]);
        }
        SetKeyRange(key_min, key_max);
        std::string metadata;
        for(int i = 0; i < col_num; i++) {
            util::PutFixed(metadata, col_size[i]);
        }
        util::PutFixed(metadata, key_num);
        util::PutFixed(metadata, col_num);
        util::PutFixed(metadata, key_size);
        writer->append(metadata.data(), metadata.size());
        writer->flush();
    }
}