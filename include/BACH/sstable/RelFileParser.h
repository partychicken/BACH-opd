#pragma once

#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <sul/dynamic_bitset.hpp>
#include "BloomFilter.h"
#include "BlockParser.h"
#include "FileMetaData.h"
#include "BACH/file/FileReader.h"
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/Options.h"
#include "BACH/common/tuple.h"
#include "BACH/sstable/BloomFilter.h"
#include "BACH/common/dictionary.h"
#include "BACH/compress/ordered_dictionary.h"

#pragma once

namespace BACH {
    template<typename Key_t>
    struct RelFileMetaData;

    template<typename Key_t>
    struct BlockMetaT {
        //std::shared_ptr<BloomFilter> filter;
        Key_t key_min; // key_max;
        size_t offset_in_file;
        size_t block_size;
    };

    template<typename Key_t>
    class RelFileParser {
    public:
        RelFileParser(FileReader *_fileReader, std::shared_ptr<Options> _options, size_t _file_size,
                      RelFileMetaData<Key_t> *_meta): reader(_fileReader), options(_options),
                                                      file_size(_file_size) {
            auto key_size = Options::KEY_SIZE;
            //
            // size_t header_size = 2 * key_size + sizeof(size_t) + 3 * sizeof(idx_t);
            // char infobuf[header_size + 10];
            // if (!reader->rread(infobuf, header_size, header_size)) {
            //     std::cout << "read fail begin" << std::endl;
            //     ++*(int *) NULL;
            // }
            key_min = _meta->key_min;
            key_max = _meta->key_max;
            key_num = _meta->key_num;
            col_num = _meta->col_num;
            block_count = _meta->block_count;
            block_meta_begin_pos = _meta->block_meta_begin_pos;
            // block_filter_size = _meta->block_filter_size;
            // block_func_num = _meta->block_func_num;
            // last_block_filter_size = _meta->last_block_filter_size;

            // size_t meta_size = 2 * key_size + 3 * sizeof(size_t);
            size_t meta_size = key_size + 2 * sizeof(size_t);
            size_t now_meta_offset = block_meta_begin_pos;
            // size_t tot_meta_size = (meta_size + block_filter_size) * (block_count - 1) + meta_size +
            //                        last_block_filter_size;
            size_t tot_meta_size = (meta_size) * (block_count - 1) + meta_size;
            char blockinfobuf[tot_meta_size];
            if (!reader->fread(blockinfobuf, tot_meta_size, now_meta_offset)) {
                std::cout << "read fail begin" << std::endl;
                ++*(int *) NULL;
            }
            block_meta.resize(block_count);
            now_meta_offset = 0;
            for (idx_t i = 0; i < block_count; i++) {
                BlockMetaT<Key_t> meta{
                    /*std::make_shared<BloomFilter>(),*/ "", 0, 0
                };
                util::DecodeFixed(blockinfobuf + now_meta_offset, meta.key_min);
                // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size, meta.key_max);
                // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size * 2, meta.offset_in_file);
                // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size * 2 + sizeof(size_t), meta.block_size);
                // std::string filters(blockinfobuf + now_meta_offset + meta_size, block_filter_size);
                // meta.filter->create_from_data(block_func_num, filters);
                util::DecodeFixed(blockinfobuf + now_meta_offset + key_size, meta.offset_in_file);
                util::DecodeFixed(blockinfobuf + now_meta_offset + key_size + sizeof(size_t), meta.block_size);
                block_meta[i] = std::move(meta);

                // now_meta_offset += meta_size + block_filter_size;
                now_meta_offset += meta_size;
            }
            // BlockMetaT<Key_t> meta{/*std::make_shared<BloomFilter>(),*/ "", 0, 0};
            // util::DecodeFixed(blockinfobuf + now_meta_offset, meta.key_min);
            // //util::DecodeFixed(blockinfobuf + now_meta_offset + key_size, meta.key_max);
            // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size * 2, meta.offset_in_file);
            // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size * 2 + sizeof(size_t), meta.block_size);
            // std::string filters(blockinfobuf + now_meta_offset + meta_size, last_block_filter_size);
            // meta.filter->create_from_data(block_func_num, filters);
            // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size, meta.offset_in_file);
            // util::DecodeFixed(blockinfobuf + now_meta_offset + key_size + sizeof(size_t), meta.block_size);
            // block_meta[block_count - 1]=std::move(meta);
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
                if (key < meta.key_min) {
                    continue;
                }
                if (i != block_count - 1 && key >= block_meta[i + 1].key_min) {
                    continue;
                }
                //if(meta.filter->exists(key)) {
                if (true) {
                    BlockParser<Key_t> block_parser(reader, options,
                                                    meta.offset_in_file, meta.block_size, col_num);
                    Tuple res = block_parser.GetTuple(key);
                    if (res.row.size() > 0) {
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
                                                meta.offset_in_file, meta.block_size, col_num);
                idx_t block_key_num = block_parser.key_num;
                block_parser.GetKeyCol(keys + idx);
                key_num += block_key_num;
                idx += block_key_num;
            }
        }

        void GetValCol(idx_t *vals, idx_t &val_num, idx_t col_id) {
            int idx = 0;
            for (idx_t i = 0; i < block_count; i++) {
                BlockMetaT<Key_t> &meta = block_meta[i];
                BlockParser<Key_t> block_parser(reader, options,
                                                meta.offset_in_file, meta.block_size, col_num);
                idx_t *block_vals = block_parser.GetValCol(col_id);
                idx_t block_key_num = block_parser.key_num;
                for (idx_t j = 0; j < block_key_num; j++) {
                    vals[idx++] = block_vals[j];
                }
                val_num += block_key_num;
            }
        }

        static bool CompareKey(const Key_t &key, const BlockMetaT<Key_t> &meta) {
            return key < meta.key_min;
        }

        std::vector<Tuple> GetKTuple(idx_t &k, Key_t key) {
            idx_t &now_k = k;
            std::vector<Tuple> res;
            res.reserve(k);
            auto iter = upper_bound(block_meta.begin(), block_meta.end(), key, CompareKey);
            // if (iter == block_meta.end()) {
            //     return res;
            // }
            if (iter != block_meta.begin()) --iter;
            for (; iter != block_meta.end(); ++iter) {
                BlockMetaT<Key_t> &meta = *iter;
                if (iter != block_meta.end() - 1 && key >= (iter + 1)->key_min) continue;
                BlockParser<Key_t> block_parser(reader, options,
                                                meta.offset_in_file, meta.block_size, col_num);
                block_parser.GetKTuple(now_k, key, res);
                if (!now_k) break;
            }
            return res;
        }

        idx_t GetBlock(Key_t key, BlockParser<Key_t> *&block) {
            auto iter = upper_bound(block_meta.begin(), block_meta.end(), key, CompareKey);
            if (iter != block_meta.begin()) --iter;
            if (iter != block_meta.end() - 1 && key >= (iter + 1)->key_min) ++iter;
            BlockMetaT<Key_t> &meta = *iter;
            block = new BlockParser<Key_t>(reader, options, meta.offset_in_file, meta.block_size, col_num);
            return iter - block_meta.begin();
        }

        BlockParser<Key_t> *GetBlock(const idx_t &block_idx) {
            BlockMetaT<Key_t> &meta = block_meta[block_idx];
            return new BlockParser<Key_t>(reader, options, meta.offset_in_file, meta.block_size, col_num);
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
        size_t block_filter_size;
        size_t last_block_filter_size;
        idx_t block_func_num;

        std::vector<BlockMetaT<Key_t> > block_meta;

        bool valid = true;
    };

    struct FileMetaData {
        std::string file_name;
        label_t label;
        idx_t level;
        vertex_t vertex_id_b;
        idx_t file_id;
        std::atomic<idx_t> ref = 0;
        size_t file_size;
        bool deletion = false;
        bool merging = false;
        sul::dynamic_bitset<> *filter = NULL;
        std::atomic<FileReader *> reader = NULL;
        BloomFilter bloom_filter;
        idx_t reader_pos = -1;
        size_t id = 0;

        FileMetaData() = default;

        FileMetaData(FileMetaData &&x) : file_name(x.file_name), label(x.label), level(x.level),
                                         vertex_id_b(x.vertex_id_b), file_id(x.file_id), ref(x.ref.load()),
                                         file_size(x.file_size), deletion(x.deletion), reader(x.reader.load()),
                                         bloom_filter(std::move(x.bloom_filter)), id(x.id) {
        }

        FileMetaData(const FileMetaData &x) : file_name(x.file_name), label(x.label), level(x.level),
                                              vertex_id_b(x.vertex_id_b), file_id(x.file_id), ref(x.ref.load()),
                                              file_size(x.file_size), deletion(x.deletion), reader(x.reader.load()),
                                              bloom_filter(x.bloom_filter), id(x.id) {
        }

        FileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
                     idx_t _file_id, std::string_view label_name) : label(_label), level(_level),
                                                                    vertex_id_b(_vertex_id_b), file_id(_file_id),
                                                                    id((size_t) label << 56 | (size_t) level << 48 | (
                                                                           size_t) vertex_id_b << 16 | (size_t)
                                                                       file_id) {
            file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
        }
    };

    template<typename Key_t>
    struct RelFileMetaData : public FileMetaData {
        std::vector<OrderedDictionary> dictionary;
        Key_t key_min, key_max;
        idx_t key_num, col_num;

        idx_t block_count;
        size_t block_meta_begin_pos;
        size_t block_filter_size;
        size_t last_block_filter_size;
        idx_t block_func_num;
        RelFileParser<Key_t> *parser = nullptr;

        RelFileMetaData() = default;

        //RelFileMetaData(RelFileMetaData &&x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max){}
        //RelFileMetaData(const RelFileMetaData &x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max){
        //}

        RelFileMetaData(RelFileMetaData &&x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max),
                                               FileMetaData(x), key_num(x.key_num), col_num(x.col_num),
                                               block_count(x.block_count), block_meta_begin_pos(x.block_meta_begin_pos),
                                               block_filter_size(x.block_filter_size),
                                               last_block_filter_size(x.last_block_filter_size),
                                               block_func_num(x.block_func_num),
                                               parser(x.parser) {
        }

        RelFileMetaData(const RelFileMetaData &x) : FileMetaData(x), dictionary(x.dictionary), key_min(x.key_min),
                                                    key_max(x.key_max),
                                                    key_num(x.key_num), col_num(x.col_num), block_count(x.block_count),
                                                    block_meta_begin_pos(x.block_meta_begin_pos),
                                                    block_filter_size(x.block_filter_size),
                                                    last_block_filter_size(x.last_block_filter_size),
                                                    block_func_num(x.block_func_num),
                                                    parser(x.parser) {
        }

        RelFileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
                        idx_t _file_id, std::string_view label_name, Key_t _key_min, Key_t _key_max,
                        idx_t _key_num, idx_t _col_num) : FileMetaData(_label, _level, _vertex_id_b, _file_id,
                                                                       label_name),
                                                          key_min(_key_min), key_max(_key_max), key_num(_key_num),
                                                          col_num(_col_num) {
            if constexpr (std::is_same_v<Key_t, std::string>) {
                file_name = std::string(key_min.c_str()) + "-" + std::string(key_max.c_str()) + file_name;
            } else {
                file_name = std::to_string(key_min) + std::to_string(key_max) + file_name;
            }
        }
    };
}
