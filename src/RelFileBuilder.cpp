#include "BACH/sstable/RelFileBuilder.h"
#include "BACH/utils/utils.h"
#include "BACH/db/DB.h"

namespace BACH {
    template<typename Key_t>
    RelFileBuilder<Key_t>::RelFileBuilder(std::shared_ptr<FileWriter> _fileWriter,
                                      std::shared_ptr<Options> _options) :
        writer(_fileWriter),
        options(_options){}

    template<typename Key_t>
    size_t RelFileBuilder<Key_t>::SetBlock(BlockBuilder<Key_t>*block_builder, BloomFilter &filter,
                                         Key_t* keys, idx_t key_num, size_t key_size, idx_t col_num,
                                            idx_t** vals, idx_t* val_nums, idx_t* val_offset) {
          size_t tot_size = 0;
          block_builder->SetKey(keys, key_num, key_num * sizeof(key_t));
          tot_size += key_num * sizeof(key_t);
          size_t* col_size = static_cast<size_t*>(malloc(col_num * sizeof(size_t)));
          for(int i = 0; i < col_num; i++) {
              col_size[i] = block_builder->SetValueIdx(vals[i] + val_offset[i], val_nums[i]);
              tot_size += col_size[i] * val_nums[i];
          }
          block_builder->ArrangeBlockInfo(filter, keys, key_num, col_num, sizeof(key_t),
                                           col_size);
          tot_size += sizeof(size_t) * (1 + col_num) + sizeof(idx_t) * 2;
          free(col_size);
          return tot_size;
    }

    template<typename Key_t>
    size_t RelFileBuilder<Key_t>::SetBlockMeta(size_t now_offset_in_file) {
        std::string metadata;
        for(int i = 0; i < block_count; i++) {
            util::PutFixed(metadata, block_meta[i].key_min);
            util::PutFixed(metadata, block_meta[i].key_max);
            util::PutFixed(metadata, block_meta[i].offset_in_file);
            util::PutFixed(metadata, block_meta[i].block_size);
            std::string filter_data = block_meta[i].filter->data();
            util::PutFixed(metadata, static_cast<size_t>(filter_data.size()));
            metadata += filter_data;
        }
        writer->append(metadata.data(), metadata.size());
        return metadata.size();
    }

    template<typename Key_t>
    void RelFileBuilder<Key_t>::ArrangeRelFileInfo(Key_t *keys, idx_t key_num, size_t key_size,
                                                   idx_t col_num, idx_t**vals, idx_t* val_nums){
        if(!key_size) key_size = sizeof(Key_t);
        size_t tot_val_size = sizeof(idx_t) * col_num; //can be optimized
        idx_t single_block_num = options->MAX_BLOCK_SIZE / (key_size + tot_val_size);
        int block_cnt = (key_num - 1) / single_block_num + 1;
        idx_t now_ptr = 0;
        //size_t* val_offset = static_cast<size_t*>(malloc(sizeof(size_t*) * col_num));
        idx_t key_offset;
        idx_t val_offset[col_num];
        idx_t block_key_num;
        idx_t block_val_num[col_num];
        size_t now_offset_in_file = 0;

        for(int i = 0; i < block_cnt; i++) {
            if(i < block_cnt - 1) {
                block_key_num = single_block_num;
                key_offset = now_ptr;
                for(int j = 0; j < col_num; j++) {
                    val_offset[i] = now_ptr;
                    block_val_num[i] = single_block_num;
                }

            } else {
                block_key_num = key_num - single_block_num * (block_cnt - 1);
                key_offset = now_ptr;
                for(int j = 0; j < col_num; j++) {
                    val_offset[i] = now_ptr;
                    block_val_num[i] = key_num - single_block_num * (block_cnt - 1);
                }
            }
            BlockMetaT<Key_t> meta{std::make_shared<BloomFilter>, 0, 0, 0, 0};
            BlockBuilder<Key_t>* builder = new BlockBuilder<Key_t>(writer, options);
            size_t block_size = this->SetBlock(builder, meta.filter, keys + key_offset,
                                         block_key_num, key_size, vals, block_val_num, val_offset);
            meta.offset_in_file = now_offset_in_file;
            meta.block_size = block_size;
            meta.key_min = builder->key_min;
            meta.key_max = builder->key_max;
            block_meta.push_back(meta);

            now_offset_in_file += block_size;
            now_ptr += single_block_num;
            delete builder;
        }

        block_count = block_cnt;
        block_meta_begin_pos = now_offset_in_file;
        now_offset_in_file += this->SetBlockMeta(now_offset_in_file);

        if(global_filter != nullptr) {
            //put global filter here
            //temporarily not put it in
        }
        //put file meta here
        size_t header_size = 2 * sizeof(key_t) + sizeof(size_t) + 3 * sizeof(idx_t);
        std::string meta_data;
        util::PutFixed(meta_data, key_min);
        util::PutFixed(meta_data, key_max);
        util::PutFixed(meta_data, key_num);
        util::PutFixed(meta_data, col_num);
        util::PutFixed(meta_data, block_count);
        util::PutFixed(meta_data, block_meta_begin_pos);
        file_size = now_offset_in_file + header_size;
        writer->flush();
    }
}

