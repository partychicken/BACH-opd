//
// Created by wangziyao on 25-5-27.
//

#include "BACH/common/ScanIter.h"

#include <BACH/memory/RowMemoryManager.h>

namespace BACH {
    ScanIter::ScanIter(RelVersion *rel_version, std::shared_ptr<relMemTable> x, std::string key)
        : rel_version(rel_version) {
        while (x) {
            mem_ptr_list.emplace_back(x, key);
            hold.emplace_back(x);
            if (x == rel_version->size_entry) break;
            x = x->next.lock();
        }
        mem_ptr_num = mem_ptr_list.size();
        block_ptr_list = new BlockPointer[20];
        std::string KEY_MAX;
        for (unsigned int i = 0; i < 16; i++) KEY_MAX += ((char) 127);
        RelVersionIterator iter(rel_version, key, KEY_MAX);
        while (!iter.End()) {
            auto now = &block_ptr_list[block_ptr_num];
            now->level = iter.GetLevel();
            now->file_idx = iter.GetIdx();
            now->block_idx = iter.GetFile()->parser->GetBlock(key, now->block);
            now->idx = now->block->GetIdx(key);
            now->now_key = now->block->GetKeyWithIdx(now->idx);

            if (now->level) iter.nextlevel();
            else iter.next();
            block_ptr_num++;
        }

        GetNow();
    }

    void ScanIter::GetTuple(Tuple &res) {
        if (now_least_type == 1) {
            auto &it = mem_ptr_list[now_least_pos].iter;
            if (!mem_ptr_list[now_least_pos].end()) {
                auto found = it->second;
                res = hold[now_least_pos]->tuple_pool[found]->tuple;
            }
        } else if (now_least_type == 2) {
            auto &now = block_ptr_list[now_least_pos];
            res = now.block->GetTupleWithIdx(std::string(now.now_key), now.idx);
            for (size_t i = 1; i < res.row.size(); i++) {
                idx_t col_id = *((idx_t *) res.row[i].data());
                res.row[i] = rel_version->FileIndex[now.level][now.file_idx]->dictionary[i - 1].getString(col_id);
            }
        }
    }

    void ScanIter::GetNow() {
        std::string now_min;
        bool all_end = true;
        for (unsigned int i = 0; i < 16; i++) now_min += ((char) 127);
        for (int i = 0; i < mem_ptr_num; i++) {
            auto &now = mem_ptr_list[i];
            if (!now.end() && now.now_key < now_min) {
                all_end = false;
                now_min = now.now_key;
                now_least_type = 1;
                now_least_pos = i;
            }
        }

        for (int i = 0; i < block_ptr_num; i++) {
            auto &now = block_ptr_list[i];
            if (!now.end() && now.now_key < now_min) {
                all_end = false;
                now_min = now.now_key;
                now_least_type = 2;
                now_least_pos = i;
            }
        }
        if (all_end) {
            is_end = true;
        }
    }

    void ScanIter::next() {
        std::string now_min;
        if (now_least_type == 1) {
            now_min = mem_ptr_list[now_least_type].now_key;
        } else if (now_least_type == 2) {
            now_min = block_ptr_list[now_least_pos].now_key;
        } else {
            exit(233);
        }
        for (int i = 0; i < mem_ptr_num; i++) {
            auto &now = mem_ptr_list[i];
            if (!now.end() && now.now_key == now_min) {
                now.next();
            }
        }

        for (int i = 0; i < block_ptr_num; i++) {
            auto &now = block_ptr_list[i];
            if (!now.end() && now.now_key == now_min) {
                now.next(rel_version);
            }
        }

        GetNow();
    }
} // BACH
