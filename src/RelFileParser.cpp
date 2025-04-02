#include "BACH/sstable/RelFileParser.h"
#include "BACH/db/DB.h"

namespace BACH {
    template<typename Key_t>
    RelFileParser<Key_t>::RelFileParser(std::shared_ptr<FileReader> _fileReader,
               std::shared_ptr<Options> _options, size_t _file_size) :
               reader(_fileReader), options(_options), file_size(_file_size) {
        size_t header_size = 2 * sizeof(key_t) + sizeof(size_t) + 3 * sizeof(idx_t);
        char infobuf[header_size];
        if(!reader->rread(infobuf, header_size, header_size)) {
            std::cout << "read fail begin" << std::endl;
            ++*(int *)NULL;
        }
        util::DecodeFixed(infobuf, key_min);
        util::DecodeFixed(infobuf + sizeof(Key_t), key_max);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2, key_num);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(idx_t), col_num);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(idx_t) * 2, block_count);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(idx_t) * 3, block_meta_begin_pos);

        size_t meta_size = 2 * sizeof(key_t) + 3 * sizeof(size_t);
        size_t now_meta_offset = block_meta_begin_pos;
        for(int i = 0; i < block_count; i++) {
            BlockMetaT<Key_t> meta{std::make_shared<BloomFilter>, 0, 0, 0, 0};
            char infobuf[meta_size];
            if(!reader->fread(infobuf, now_meta_offset, meta_size)) {
                std::cout << "read fail begin" << std::endl;
                ++*(int *)NULL;
            }
            size_t filter_size = 0;
            util::DecodeFixed(infobuf, meta.key_min);
            util::DecodeFixed(infobuf + sizeof(Key_t), meta.key_max);
            util::DecodeFixed(infobuf + sizeof(Key_t) * 2, meta.offset_in_file);
            util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(size_t), meta.block_size);
            util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(size_t) * 2, filter_size);
            char filterbuf[filter_size + 1];
            filterbuf[filter_size] = 0;
            if(!reader->fread(filterbuf, now_meta_offset + meta_size, filter_size)) {
                std::cout << "read fail begin" << std::endl;
                ++*(int *)NULL;
            }
            meta.filter->create_from_data(1, filterbuf);
            block_meta.push_back(meta);

            now_meta_offset += meta_size + filter_size;
        }
    }

    template<typename Key_t>
    Tuple RelFileParser<Key_t>::GetTuple(Key_t key) {
        for(int i = 0; i < block_count; i++) {
            BlockMetaT<Key_t> &meta = block_meta[i];
            if(key < meta.key_min || key > meta.key_max) {
                continue;
            }
            if(meta.filter->exist(key)) {
                BlockParser<Key_t> block_parser(reader, options,
                                 meta.offset_in_file, meta.block_size);
                Tuple res = block_parser.GetTuple(key);
                if(res.col_num) {
                    return res;
                }
            }
        }
        return Tuple();
    }

    template<typename Key_t>
    void RelFileParser<Key_t>::GetKeyCol(Key_t *keys, idx_t &key_num) {
        int idx = 0;
        for(int i = 0; i < block_count; i++) {
            BlockMetaT<Key_t> &meta = block_meta[i];
            BlockParser<Key_t> block_parser(reader, options,
                             meta.offset_in_file, meta.block_size);
            Key_t* block_key = block_parser.GetKeyCol();
            idx_t block_key_num = block_parser.key_num;
            for(idx_t j = 0; j < block_key_num; j++) {
                keys[idx++] = block_key[j];
            }
            key_num += block_key_num;
            free(block_key);
        }
    }

    template<typename Key_t>
    void RelFileParser<Key_t>::GetValCol(idx_t *vals, idx_t &val_num, idx_t col_id) {
        int idx = 0;
        for(int i = 0; i < block_count; i++) {
            BlockMetaT<Key_t> &meta = block_meta[i];
            BlockParser<Key_t> block_parser(reader, options,
                             meta.offset_in_file, meta.block_size);
            idx_t* block_vals = block_parser.GetValCol(col_id);
            idx_t block_key_num = block_parser.key_num;
            for(idx_t j = 0; j < block_key_num; j++) {
                vals[idx++] = block_vals[j];
            }
            val_num += block_key_num;
            free(block_vals);
        }
    }
}