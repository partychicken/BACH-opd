#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <sul/dynamic_bitset.hpp>
#include "BACH/file/FileReader.h"
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"
#include "BACH/sstable/BloomFilter.h"
#include "BACH/common/dictionary.h"
#include "BACH/compress/ordered_dictionary.h"

namespace BACH {
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

        RelFileMetaData() = default;

        //RelFileMetaData(RelFileMetaData &&x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max){}
        //RelFileMetaData(const RelFileMetaData &x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max){
        //}

        RelFileMetaData(RelFileMetaData &&x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max),
                                               FileMetaData(x), key_num(x.key_num), col_num(x.col_num), block_count(x.block_count), block_meta_begin_pos(x.block_meta_begin_pos),
                                               block_filter_size(x.block_filter_size),last_block_filter_size(x.last_block_filter_size), block_func_num(x.block_func_num) {
        }

        RelFileMetaData(const RelFileMetaData &x) : FileMetaData(x), dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max),
        key_num(x.key_num), col_num(x.col_num),block_count(x.block_count), block_meta_begin_pos(x.block_meta_begin_pos),
            block_filter_size(x.block_filter_size), last_block_filter_size(x.last_block_filter_size), block_func_num(x.block_func_num) {
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
