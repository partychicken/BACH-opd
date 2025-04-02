#pragma once

#include <memory>
#include <vector>
#include <utility>
#include <sul/dynamic_bitset.hpp>
#include "BloomFilter.h"
#include "BACH/file/FileWriter.h"
#include "BACH/memory/VertexEntry.h"
#include "BlockBuilder.h"
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"

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
    class RelFileBuilder
    {
    public:
        explicit RelFileBuilder(std::shared_ptr<FileWriter> _fileWriter,
                              std::shared_ptr<Options> _options);
        ~RelFileBuilder() = default;
        //void AddFilter(idx_t keys_num, double false_positive);

        void ArrangeRelFileInfo(Key_t *keys, idx_t key_num, size_t key_size,
                                idx_t col_num, idx_t**vals);

        void SetDict();//optional
        void SetBloomFilter();
    private:
        std::shared_ptr <FileWriter> writer;
        std::shared_ptr<Options> options;
        std::vector<BlockMetaT<Key_t> > block_meta;
        size_t file_size;
        size_t block_meta_begin_pos;
        Key_t key_min, key_max;
        idx_t block_count;
        std::shared_ptr<BloomFilter>global_filter = nullptr;

        size_t SetBlock(BlockBuilder<Key_t>*block_builder, BloomFilter &filter,
                              Key_t* keys, idx_t key_num, size_t key_size, idx_t col_num,
                                 idx_t** vals, idx_t* val_nums, idx_t *val_offset);
        size_t SetBlockMeta(size_t offset);
    };
}
