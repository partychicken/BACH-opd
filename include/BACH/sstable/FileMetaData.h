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
        idx_t reader_pos = -1;
        size_t id = 0;

        FileMetaData() = default;

        FileMetaData(FileMetaData &&x) : file_name(x.file_name), label(x.label), level(x.level),
                                         vertex_id_b(x.vertex_id_b), file_id(x.file_id), ref(x.ref.load()),
                                         file_size(x.file_size), deletion(x.deletion), filter(x.filter),
                                         reader(x.reader.load()), id(x.id) {
        }

        FileMetaData(const FileMetaData &x) : file_name(x.file_name), label(x.label), level(x.level),
                                         vertex_id_b(x.vertex_id_b), file_id(x.file_id), ref(x.ref.load()),
                                         file_size(x.file_size), deletion(x.deletion), filter(x.filter),
                                         reader(x.reader.load()), id(x.id) {
        }

        FileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
                     idx_t _file_id, std::string_view label_name) :
                     label(_label), level(_level), vertex_id_b(_vertex_id_b), file_id(_file_id),
                     id((size_t) label << 56 | (size_t) level << 48 | (size_t) vertex_id_b << 16 | (size_t) file_id) {
            file_name = util::BuildSSTPath(label_name, level, vertex_id_b, file_id);
        }
    };

    template<typename Key_t>
    struct RelFileMetaData : public FileMetaData {
        std::vector<OrderedDictionary> dictionary;
        Key_t key_min, key_max;
        idx_t key_num, col_num;

        RelFileMetaData() = default;

        //RelFileMetaData(RelFileMetaData &&x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max){}
        //RelFileMetaData(const RelFileMetaData &x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max){
        //}
        
        RelFileMetaData(RelFileMetaData&& x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max), 
        FileMetaData(x), key_num(x.key_num), col_num(x.col_num){
        }
        RelFileMetaData(const RelFileMetaData &x) : dictionary(x.dictionary), key_min(x.key_min), key_max(x.key_max),
            FileMetaData(x), key_num(x.key_num), col_num(x.col_num) {
        }

        RelFileMetaData(label_t _label, idx_t _level, vertex_t _vertex_id_b,
                     idx_t _file_id, std::string_view label_name, Key_t _key_min, Key_t _key_max,
                     idx_t _key_num, idx_t _col_num) :
                     FileMetaData(_label, _level, _vertex_id_b, _file_id, label_name),
                     key_min(_key_min), key_max(_key_max), key_num(_key_num), col_num(_col_num) {
            if constexpr (std::is_same_v<Key_t, std::string>) {
				file_name += key_min + key_max;
            }
            else {
                file_name += std::to_string(key_min) + std::to_string(key_max);
            }
        }
    };
}
