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

namespace BACH
{
    template<typename Key_t>
	class BlockParser
	{
	public:
		BlockParser( std::shared_ptr<FileReader> _fileReader,
			std::shared_ptr<Options> _options, size_t _offset_in_file,
            size_t _block_size);
        BlockParser(BlockParser &&x);

        ~BlockParser();

        Tuple GetTuple(Key_t key);


        //output raw pointers, needing free outside after copy
        Key_t* GetKeyCol(); 
        idx_t* GetValCol(int col_id);

        size_t GetKeySize() {
            return key_size;
        }
        size_t* GetValueSize() {
            return col_size;
        }
        idx_t GetKeyNum() {
            return key_num;
        }
        idx_t GetColumnNum() {
            return col_num;
        }

	private:
		std::shared_ptr<FileReader> reader;
		std::shared_ptr<Options> options;

    	Tuple GetTupleWithIdx(Key_t key, idx_t idx);
        
        size_t offset_in_file = 0;
        size_t block_size = 0;
		//std::shared_ptr <BloomFilter> filter = NULL;
        idx_t key_num = 0, col_num = 0;
        size_t key_size = 0;
        size_t* col_size = nullptr;
        size_t key_data_endpos = 0;
        size_t* col_data_endpos = nullptr;
		bool valid = true;
	};
}