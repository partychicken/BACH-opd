#pragma once

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include "BloomFilter.h"
#include "BACH/file/FileReader.h"
#include "BACH/utils/utils.h"
#include "BACH/utils/Options.h"
#include "BACH/memory/VertexEntry.h"
#include "BACH/common/tuple.h"

#pragma once

namespace BACH {
    template<typename Key_t>
    class RelFileParser;

    template<typename Key_t>
    class BlockParser {
    public:
        BlockParser(FileReader *_fileReader,
                    std::shared_ptr<Options> _options, size_t _offset_in_file,
                    size_t _block_size, idx_t _col_num): reader(_fileReader), options(_options),
                                         offset_in_file(_offset_in_file), block_size(_block_size), col_num(_col_num) {
            size_t block_end = block_size;
            //size_t offset = 2 * sizeof(idx_t) + sizeof(size_t) + col_num * sizeof(size_t);
            size_t offset = 2 * sizeof(idx_t) + sizeof(size_t);
            data_buffer = static_cast<char*>(malloc(block_size));
            if (!reader->fread(data_buffer, block_size, offset_in_file)) {
                std::cout << "read fail begin" << std::endl;
                ++*(int *) NULL;
            }
            char *infobuf = data_buffer + block_end - offset;
            util::DecodeFixed(infobuf, key_num);
            util::DecodeFixed(infobuf + sizeof(idx_t), col_num);
            util::DecodeFixed(infobuf + sizeof(idx_t) * 2, key_size);
            size_t col_size_count = col_num * sizeof(size_t);
            size_t col_size_beginpos = block_end - (offset + col_size_count);
            char *colbuf = data_buffer + col_size_beginpos;
            col_size = new size_t[col_num];
            for (idx_t i = 0; i < col_num; i++) {
                util::DecodeFixed(colbuf, col_size[i]);
            }
            key_data_endpos = key_size * key_num;
            size_t nowpos = key_data_endpos;
            col_data_endpos = static_cast<size_t *>(malloc(col_num * sizeof(size_t)));
            for (idx_t i = 0; i < col_num; i++) {
                nowpos += key_num * col_size[i];
                col_data_endpos[i] = nowpos;
            }
        }

        BlockParser(BlockParser &&x): reader(x.reader), options(x.options), key_num(x.key_num),
                                      col_num(x.col_num), key_size(x.key_size), col_size(x.col_size),
                                      key_data_endpos(x.key_data_endpos), col_data_endpos(x.col_data_endpos),
                                      offset_in_file(x.offset_in_file), block_size(x.block_size) {
            x.valid = false;
        }

        ~BlockParser() {
            free(data_buffer);
            free(col_data_endpos);
            delete[] col_size;
        }

        Tuple GetTuple(Key_t key) {
            char* buffer = data_buffer;
            idx_t tmp_read_num = key_num;
            size_t inner_off = 0;
            // for (idx_t j = 0; j < tmp_read_num; j++) {
            //     Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + inner_off);
            //     if (nowkey == key) {
            //         return GetTupleWithIdx(key, j);
            //     }
            //     inner_off += key_size;
            // }
            idx_t l = 0, r = key_num - 1;
            while (l < r) {
                idx_t mid = (l + r) >> 1;
                Key_t midkey = util::GetDecodeFixed<Key_t>(buffer + mid * key_size);
                if (midkey < key) l = mid + 1;
                else r = mid;
            }
            Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + l * key_size);
            if (nowkey == key) return GetTupleWithIdx(key, l);
            return Tuple();
        }

        void GetKeyCol(Key_t* res) {
            char *buffer = data_buffer;
            for (idx_t i = 0; i < key_num; i++) {
                res[i] = std::string(buffer + key_size * i, key_size);
            }
        }

        //output raw pointers, needing free outside after copy
        idx_t *GetValCol(int col_id) {
            if (col_id) {
                char *buffer = data_buffer + col_data_endpos[col_id - 1];
                return reinterpret_cast<idx_t *>(buffer);
            } else {
                char *buffer = data_buffer + key_data_endpos;
                return reinterpret_cast<idx_t *>(buffer);
            }
        }

        void GetKTuple(idx_t &k, Key_t key, std::vector<Tuple> &res) {
            char* buffer = data_buffer;
            size_t inner_off = 0;
            for (idx_t j = 0; j < key_num; j++) {
                Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + inner_off);
                if (nowkey >= key) {
                    res.emplace_back(GetTupleWithIdx(nowkey, j));
                    k--;
                    if (!k) {
                        return;
                    }
                }
                inner_off += key_size;
            }
        }


        size_t GetKeySize() {
            return key_size;
        }

        size_t *GetValueSize() {
            return col_size;
        }

        idx_t GetKeyNum() {
            return key_num;
        }

        idx_t GetColumnNum() {
            return col_num;
        }

    private:
        FileReader *reader;
        std::shared_ptr<Options> options;

        Tuple GetTupleWithIdx(Key_t key, const idx_t &idx) {
            Tuple result;
            result.col_num = col_num + 1;
            if constexpr (!std::is_same_v<Key_t, std::string>) result.row.push_back(key.to_string());
            else result.row.push_back(key);
            for (idx_t i = 0; i < col_num; i++) {
                size_t now_size = col_size[i];
                size_t now_begin = i ? col_data_endpos[i - 1] : key_data_endpos;
                result.row.push_back(std::string(data_buffer + now_begin + idx * now_size, now_size));
            }
            return result;
        }

        size_t offset_in_file = 0;
        size_t block_size = 0;
        //std::shared_ptr <BloomFilter> filter = NULL;
        idx_t key_num = 0, col_num = 0;
        size_t key_size = Options::KEY_SIZE;
        size_t *col_size = nullptr;
        size_t key_data_endpos = 0;
        size_t *col_data_endpos = nullptr;
        char *data_buffer = nullptr;
        bool valid = true;

        friend class RelFileParser<Key_t>;
    };
}
