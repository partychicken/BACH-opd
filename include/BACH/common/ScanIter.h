#pragma once
#include "BACH/sstable/BlockParser.h"
#include "BACH/sstable/RelVersion.h"
#include "BACH/memory/RowMemoryManager.h"

namespace BACH {
    class ScanIter {
    public:
        ScanIter(RelVersion *rel_version,  std::shared_ptr<relMemTable> x, std::string key);

        void next();

        bool end() const {
            return is_end;
        }

        void GetNow();

        void GetTuple(Tuple &res);

    private:
        struct BlockPointer {
            bool is_end = false;
            idx_t level, file_idx;
            idx_t block_idx, idx;
            BlockParser<std::string> *block = nullptr;
            std::string now_key;

            ~BlockPointer() {
                if (block) delete block;
            }

            void next(const RelVersion *rel_version) {
                idx++;
                if (idx >= block->GetKeyNum()) {
                    idx = 0;
                    block_idx++;
                    delete block;
                    if (block_idx >= rel_version->FileIndex[level][file_idx]->block_count) {
                        block_idx = 0;
                        file_idx++;
                        if (level == 0 || file_idx >= rel_version->FileIndex[level].size()) {
                            is_end = true;
                            return;
                        }
                    }
                    block = rel_version->FileIndex[level][file_idx]->parser->GetBlock(block_idx);
                }
                now_key = block->GetKeyWithIdx(idx);
            }

            bool end() const {
                return is_end;
            }
        };

        struct MemPointer {
            std::string now_key;
            RelSkipList::Accessor accessor;
            RelSkipList::Accessor::iterator iter;
            bool is_end;

            MemPointer(std::shared_ptr<relMemTable> mem_table, const std::string &key): accessor(mem_table->tuple_index) {
                iter = accessor.lower_bound(std::make_pair(key, 0));
                if (iter == accessor.end()) {
                    is_end = true;
                }
                else now_key = iter->first;
            }

            void next() {
                if (iter != accessor.end()) {
                    ++iter;
                    now_key = iter->first;
                } else
                    is_end = true;
            }

            bool end() const {
                return is_end;
            }
        };

        BlockPointer *block_ptr_list;
        std::vector<MemPointer> mem_ptr_list;

        int now_least_type; // 0 indicated invalid, 1 indicated in mem-list, 2 in block-list
        int now_least_pos; //pos in the list

        int mem_ptr_num = 0;
        int block_ptr_num = 0;
        RelVersion *rel_version;
        rowMemoryManager *memory_manager;

        std::vector<std::shared_ptr<relMemTable> > hold;
        bool is_end;
    };
} // BACH
