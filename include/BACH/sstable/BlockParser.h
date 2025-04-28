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
	template<typename Key_t>class RelFileParser;
    template<typename Key_t>
	class BlockParser
	{
	public:
		BlockParser( FileReader* _fileReader,
			std::shared_ptr<Options> _options, size_t _offset_in_file,
			size_t _block_size): reader(_fileReader), options(_options),
			offset_in_file(_offset_in_file),block_size(_block_size) {
			size_t block_end = offset_in_file + block_size;
			size_t offset = 2 * sizeof(idx_t) + sizeof(size_t);
			char infobuf[offset];
			if(!reader->fread(infobuf, offset, block_end - offset)) {
				std::cout << "read fail begin" << std::endl;
				++*(int *)NULL;
			}
			util::DecodeFixed(infobuf, key_num);
			util::DecodeFixed(infobuf + sizeof(idx_t), col_num);
			util::DecodeFixed(infobuf + sizeof(idx_t) * 2, key_size);
			size_t col_size_count = col_num * sizeof(size_t);
			size_t col_size_beginpos =  block_end - (offset + col_size_count);
			char colbuf[col_size_count];
			if(!reader->fread(infobuf, col_size_count, col_size_beginpos)) {
				std::cout << "read fail begin colsiz" << std::endl;
				++*(int *)NULL;
			}
			for(idx_t i = 0; i < col_num; i++) {
				util::DecodeFixed(colbuf, col_size[i]);
			}

			key_data_endpos = key_size * key_num + offset_in_file;
			size_t nowpos = key_data_endpos + offset_in_file;
			col_data_endpos = static_cast<size_t*>(malloc(col_num * sizeof(size_t)));
			for(idx_t i = 0; i < col_num; i++) {
				col_data_endpos[i] = nowpos + key_num * col_size[i];
			}
		}
    	BlockParser(BlockParser &&x):
			reader(x.reader), options(x.options), key_num(x.key_num),
			col_num(x.col_num), key_size(x.key_size), col_size(x.col_size),
			key_data_endpos(x.key_data_endpos), col_data_endpos(x.col_data_endpos),
			offset_in_file(x.offset_in_file), block_size(x.block_size) {
			x.valid = false;
		}

		~BlockParser() {
			if(valid) {
				reader->DecRef();
			}
			free(col_data_endpos);
			free(col_size);
		}

		Tuple GetTuple(Key_t key) {
			idx_t single_read_num = this->options->READ_BUFFER_SIZE / key_size;
			size_t single_read_size = single_read_num * key_size;
			int read_times = (key_num - 1) / single_read_num + 1;
			size_t offset = 0;
			if constexpr (std::is_same_v<std::string, Key_t>) {
				key.resize(Options::KEY_SIZE);
			}
			for(int i = 0; i < read_times - 1; i++) {
				char buffer[single_read_size];
				if (!reader->fread(buffer, single_read_size, offset)) {
					std::cout << "read fail dst" << std::endl;
					++*(int *)NULL;
				}
				offset += single_read_size;
				size_t inner_off = 0;
				for(idx_t j = 0; j < single_read_num; j++) {
					Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + inner_off);
					if(nowkey == key) {
						return GetTupleWithIdx(key, i * single_read_num + j);
					}
					inner_off += key_size;
				}
			}
			//specific for the last round
			char buffer[single_read_size];
			idx_t tmp_read_num = (key_data_endpos - offset) / key_size;
			if (!reader->fread(buffer, key_data_endpos - offset, offset)) {
				std::cout << "read fail dst" << std::endl;
				++*(int *)NULL;
			}
			size_t inner_off = 0;
			for(idx_t j = 0; j < tmp_read_num; j++) {
				Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + inner_off);
				if(nowkey == key) {
					return GetTupleWithIdx(key, (read_times - 1) * single_read_num + j);
				}
				inner_off += key_size;
			}
			return Tuple();
		}

		//output raw pointers, needing free outside after copy
		Key_t* GetKeyCol() {
			//needs seperately buffering ?
			char* buffer = static_cast<char*>(malloc(key_num * key_size));
			if (!reader->fread(buffer, key_num * key_size, 0)) {
				std::cout << "read fail dst" << std::endl;
				++*(int *)NULL;
			}
			return reinterpret_cast<Key_t*>(buffer);
		}

		//output raw pointers, needing free outside after copy
		idx_t* GetValCol(int col_id) {
			if(col_id) {
				char* buffer = static_cast<char*>(malloc(key_num * col_size[col_id]));
				if (!reader->fread(buffer, key_num * col_size[col_id], col_data_endpos[col_id - 1])) {
					std::cout << "read fail dst" << std::endl;
					++*(int *)NULL;
				}
				return reinterpret_cast<idx_t*>(buffer);
			} else {
				char* buffer = static_cast<char*>(malloc(key_num * col_size[col_id]));
				if (!reader->fread(buffer, key_num * col_size[col_id], key_data_endpos)) {
					std::cout << "read fail dst" << std::endl;
					++*(int *)NULL;
				}
				return reinterpret_cast<idx_t*>(buffer);
			}
		}


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
		FileReader* reader;
		std::shared_ptr<Options> options;

		Tuple GetTupleWithIdx(Key_t key, idx_t idx) {
    		Tuple result;
    		result.col_num = col_num + 1;
    		if constexpr(!std::is_same_v<Key_t, std::string>) result.row.push_back(key.to_string());
    		for(idx_t i = 0; i < col_num; i++) {
    			size_t now_size = col_size[i];
    			size_t now_begin = i ? col_data_endpos[i - 1] : key_data_endpos;
    			char buffer[now_size + 1];
    			if(!reader->fread(buffer, now_size, now_begin + idx * now_size)) {
    				std::cout << "read fail dst" << std::endl;
    				++*(int *)NULL;
    			}
    			result.row.push_back(std::string(buffer));
    		}
    		return result;
    	}

    	size_t offset_in_file = 0;
        size_t block_size = 0;
		//std::shared_ptr <BloomFilter> filter = NULL;
        idx_t key_num = 0, col_num = 0;
        size_t key_size = Options::KEY_SIZE;
        size_t* col_size = nullptr;
        size_t key_data_endpos = 0;
        size_t* col_data_endpos = nullptr;
		bool valid = true;

    	friend class RelFileParser<Key_t>;
	};
}