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
    };

    template<typename Key_t>
    class RelFileBuilder
    {
    public:
        explicit RelFileBuilder(std::shared_ptr<FileWriter> _fileWriter,std::shared_ptr<Options> _options);
        ~RelFileBuilder() = default;
        //void AddFilter(idx_t keys_num, double false_positive);

        sul::dynamic_bitset<>*  ArrangeRelFileInfo();
        void SetBlock(BlockBuilder<Key_t>*block_builder);
        void SetDict();//optional
        void SetBloomFilter();
    private:
        std::shared_ptr <FileWriter> writer;
        std::shared_ptr<Options> options;
        std::vector<BlockMetaT<Key_t> > block_meta;
        size_t file_size;
        Key_t key_min, key_max;
        idx_t block_count;
        std::shared_ptr<BloomFilter>global_filter;

    };
}
