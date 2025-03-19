#pragma once

#include <memory>
#include <vector>
#include "BloomFilter.h"
#include "BACH/file/FileWriter.h"
#include "BACH/utils/types.h"
#include "BACH/utils/utils.h"

namespace BACH
{
    template <typename Key_t>
	class BlockBuilder 
	{
	public:
		explicit BlockBuilder(std::shared_ptr<FileWriter> _fileWriter,std::shared_ptr<Options> _options);
		~BlockBuilder() = default;
		//void AddFilter(idx_t keys_num, double false_positive);
		void SetKeyRange(Key_t key_min, Key_t key_max);
		void ArrangeBlockInfo(BloomFilter &filter, Key_t *key, idx_t key_num, idx_t col_num);
        void SetKey(Key_t* keys, idx_t key_num = 0, size_t key_size = 0);
        void SetValueIdx(idx_t* vals, idx_t val_num = 0);
	private:
		std::shared_ptr <FileWriter> writer;
		std::shared_ptr<Options> options;
        Key_t key_min, key_max;
	};
}