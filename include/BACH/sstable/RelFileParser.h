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

namespace BACH
{
    template<typename Key_t>
        struct BlockMetaT {
        std::shared_ptr <BloomFilter> filter;
        Key_t key_min, key_max;
        size_t offset_in_file;
        size_t block_size;
    };

    template<typename Key_t>
    class RelFileParser
    {
    public:
        RelFileParser( std::shared_ptr<FileReader> _fileReader,
            std::shared_ptr<Options> _options, size_t _file_size);
        RelFileParser(RelFileParser &&x);

        ~RelFileParser();

        Tuple GetTuple(Key_t key);

        //copy the columns from blockparser, and free them
        void GetKeyCol(Key_t *keys, idx_t &key_num);
        void GetValCol(idx_t *vals, idx_t &val_num, idx_t col_id);

        idx_t GetColumnNum() {
            return col_num;
        }

    private:
        std::shared_ptr<FileReader> reader;
        std::shared_ptr<Options> options;

        size_t file_size = 0;
        idx_t col_num = 0;
        idx_t block_count = 0;
        Key_t key_min, key_max;
        size_t block_meta_begin_pos;

        std::vector<BlockMetaT<Key_t> >block_meta;

        bool valid = true;
    };
}