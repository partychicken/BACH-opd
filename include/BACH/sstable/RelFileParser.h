#pragma once

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include "BloomFilter.h"
#include "BlockParser.h"
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/Options.h"
#include "BACH/common/tuple.h"

#pragma once

namespace BACH {
    template<typename Key_t>
    struct BlockMetaT {
        std::shared_ptr<BloomFilter> filter;
        Key_t key_min, key_max;
        size_t offset_in_file;
        size_t block_size;
    };

    template<typename Key_t>
    class RelFileParser {
    public:
        RelFileParser(FileReader *_fileReader,
                      std::shared_ptr<Options> _options, size_t _file_size): reader(_fileReader), options(_options),
                                                                             file_size(_file_size) {
            auto key_size = Options::KEY_SIZE;
            //
            size_t header_size = 2 * key_size + sizeof(size_t) + 3 * sizeof(idx_t);
            char infobuf[header_size + 10];
            if (!reader->rread(infobuf, header_size, header_size)) {
                std::cout << "read fail begin" << std::endl;
                ++*(int *) NULL;
            }
            util::DecodeFixed(infobuf, key_min);
            util::DecodeFixed(infobuf + key_size, key_max);
            util::DecodeFixed(infobuf + key_size * 2, key_num);
            util::DecodeFixed(infobuf + key_size * 2 + sizeof(idx_t), col_num);
            util::DecodeFixed(infobuf + key_size * 2 + sizeof(idx_t) * 2, block_count);
            util::DecodeFixed(infobuf + key_size * 2 + sizeof(idx_t) * 3, block_meta_begin_pos);

            size_t meta_size = 2 * key_size + 3 * sizeof(size_t);
            size_t now_meta_offset = block_meta_begin_pos;
            for (idx_t i = 0; i < block_count; i++) {
                BlockMetaT<Key_t> meta{std::make_shared<BloomFilter>(), "", "", 0, 0};
                char infobuf[meta_size];
                if (!reader->fread(infobuf, meta_size, now_meta_offset)) {
                    std::cout << "read fail begin" << std::endl;
                    ++*(int *) NULL;
                }
                size_t filter_size = 0;
                util::DecodeFixed(infobuf, meta.key_min);
                util::DecodeFixed(infobuf + key_size, meta.key_max);
                util::DecodeFixed(infobuf + key_size * 2, meta.offset_in_file);
                util::DecodeFixed(infobuf + key_size * 2 + sizeof(size_t), meta.block_size);
                util::DecodeFixed(infobuf + key_size * 2 + sizeof(size_t) * 2, filter_size);
                char filterbuf[filter_size + 1];
                filterbuf[filter_size] = 0;
                if (!reader->fread(filterbuf, filter_size, now_meta_offset + meta_size)) {
                    std::cout << "read fail begin" << std::endl;
                    ++*(int *) NULL;
                }
                std::string filters = filterbuf;
                meta.filter->create_from_data(1, filters);
                block_meta.push_back(meta);

                now_meta_offset += meta_size + filter_size;
            }
        }

        RelFileParser(RelFileParser &&x) : reader(x.reader), options(x.options), file_size(x.file_size),
                                           col_num(x.col_num), key_num(x.key_num), block_count(x.block_count),
                                           key_min(x.key_min), key_max(x.key_max),
                                           block_meta_begin_pos(x.block_meta_begin_pos), block_meta(x.block_meta) {
            x.valid = false;
        }

        ~RelFileParser() {
            if (valid) {
                reader->DecRef();
            }
        };

        Tuple GetTuple(Key_t key) {
            if constexpr (std::is_same_v<std::string, Key_t>) {
                key.resize(Options::KEY_SIZE);
            }
            for (idx_t i = 0; i < block_count; i++) {
                BlockMetaT<Key_t> &meta = block_meta[i];
                if (key < meta.key_min || key > meta.key_max) {
                    continue;
                }
                //if(meta.filter->exists(key)) {
                if (true) {
                    BlockParser<Key_t> block_parser(reader, options,
                                                    meta.offset_in_file, meta.block_size);
                    Tuple res = block_parser.GetTuple(key);
                    if (res.col_num) {
                        return res;
                    }
                }
            }
            return Tuple();
        }

        //copy the columns from blockparser, and free them
        void GetKeyCol(Key_t *keys, idx_t &key_num) {
            int idx = 0;
            for (idx_t i = 0; i < block_count; i++) {
                BlockMetaT<Key_t> &meta = block_meta[i];
                BlockParser<Key_t> block_parser(reader, options,
                                                meta.offset_in_file, meta.block_size);
                Key_t *block_key = block_parser.GetKeyCol();
                idx_t block_key_num = block_parser.key_num;
                for (idx_t j = 0; j < block_key_num; j++) {
                    keys[idx++] = block_key[j];
                }
                key_num += block_key_num;
                if constexpr (std::is_same_v<Key_t, std::string>) {
                    delete[] block_key;
                } else free(block_key);
            }
        }

        void GetValCol(idx_t *vals, idx_t &val_num, idx_t col_id) {
            int idx = 0;
            for (idx_t i = 0; i < block_count; i++) {
                BlockMetaT<Key_t> &meta = block_meta[i];
                BlockParser<Key_t> block_parser(reader, options,
                                                meta.offset_in_file, meta.block_size);
                idx_t *block_vals = block_parser.GetValCol(col_id);
                idx_t block_key_num = block_parser.key_num;
                for (idx_t j = 0; j < block_key_num; j++) {
                    vals[idx++] = block_vals[j];
                }
                val_num += block_key_num;
                free(block_vals);
            }
        }

        static bool CompareKey(const Key_t &key, const BlockMetaT<Key_t>&meta) {
            return key < meta.key_min;
        }

        std::vector<Tuple> GetKTuple(idx_t k, Key_t key) {
            idx_t now_k = k;
            std::vector<Tuple> res;
            auto iter = upper_bound(block_meta.begin(), block_meta.end(), key, CompareKey);
            // if (iter == block_meta.end()) {
            //     return res;
            // }
            if (iter != block_meta.begin()) --iter;
            for (;iter != block_meta.end(); ++iter) {
                BlockMetaT<Key_t> &meta = *iter;
                if (meta.key_max < key) continue;
                BlockParser<Key_t> block_parser(reader, options,
                                                meta.offset_in_file, meta.block_size);
                block_parser.GetKTuple(now_k, key, res);
                if (!now_k) break;
            }
            return res;
        }

        idx_t GetColumnNum() {
            return col_num;
        }

        idx_t GetKeyNum() {
            return key_num;
        }

    private:
        FileReader *reader;
        std::shared_ptr<Options> options;

        size_t file_size = 0;
        idx_t col_num = 0;
        idx_t key_num = 0;
        idx_t block_count = 0;
        Key_t key_min, key_max;
        size_t block_meta_begin_pos;

        std::vector<BlockMetaT<Key_t> > block_meta;

        bool valid = true;
    };
}
