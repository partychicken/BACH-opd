#pragma once

#include <memory>
#include <vector>
#include <pair>
#include <sul/dynamic_bitset.hpp>
#include "BloomFilter.h"
#include "BACH/file/FileWriter.h"
#include "BACH/memory/VertexEntry.h"
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

        void ArrangeCurrentSrcInfo();
        sul::dynamic_bitset<>*  ArrangeRelFileInfo();
        void AddEdge(vertex_t src, vertex_t dst, edge_property_t edge_property);
    private:
        std::shared_ptr <FileWriter> writer;
        std::shared_ptr<Options> options;
        std::vector<BlockMetaT<Key_t> > block_meta;
        size_t file_size;
    };
}
