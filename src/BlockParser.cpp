#include "BACH/sstable/BlockParser.h"

/*
!!!
!!! Current version doesn't consider the block offset in file, only correct in one block
!!! Current version doesn't consider bitpacking on value index
!!!
*/
namespace BACH {
    template<typename Key_t>
    BlockParser<Key_t>::BlockParser(FileReader* _fileReader,
            std::shared_ptr<Options> _options, size_t _offset_in_file,
            size_t _block_size) :
            reader(_fileReader), options(_options), 
            offset_in_file(_offset_in_file),block_size(_block_size) {
        size_t offset = 2 * sizeof(idx_t) + sizeof(size_t);
        char infobuf[offset];
        if(!reader->rread(infobuf, offset, offset)) {
            std::cout << "read fail begin" << std::endl;
            ++*(int *)NULL;
        }
        util::DecodeFixed(infobuf, key_num);
        util::DecodeFixed(infobuf + sizeof(idx_t), col_num);
        util::DecodeFixed(infobuf + sizeof(idx_t) * 2, key_size);
        col_size_count = col_num * sizeof(size_t);
        col_size_beginpos =  offset + col_size_count;
        char colbuf[col_size_count];
        if(!reader->rread(infobuf, col_size_count, col_size_beginpos)) {
            std::cout << "read fail begin colsiz" << std::endl;
            ++*(int *)NULL;
        }
        for(int i = 0; i < col_num; i++) {
            util::DecodeFixed(colbuf, col_size[i]);
        }

        key_data_endpos = key_size * key_num;
        size_t nowpos = key_data_endpos;
        col_data_endpos = static_cast<size_t*>(malloc(col_num * sizeof(size_t)));
        for(int i = 0; i < col_num; i++) {
            col_data_endpos[i] = nowpos + key_num * col_size[i];
        }
    }

    template<typename Key_t>
    BlockParser<Key_t>::BlockParser(BlockParser<Key_t> &&x) :
        reader(x.reader), options(x.options), key_num(x.key_num),
        col_num(x.col_num), key_size(x.key_size), col_size(x.col_size),
        key_data_endpos(x.key_data_endpos), col_data_endpos(x.cod_data_endpos),
        offset_in_file(x.offset_in_file), block_size(x.block_size) {
        x.valid = false;
    }

    template<typename Key_t>
    BlockParser<Key_t>::~BlockParser() {
        if(valid) {
            reader->DecRef();
        }
        free(col_data_endpos);
        free(col_size);
    }

    template<typename Key_t>
    BlockParser<Key_t>::Tuple GetTupleWithIdx(Key_t key, idx_t idx) {
        Tuple result;
        result.col_num = col_num + 1;
        result.row.push_back(key.to_string());
        for(int i = 0; i < col_num; i++) {
            size_t now_size = col_size[i];
            size_t now_begin = i ? col_data_endpos[i - 1] : key_data_endpos; 
            char buffer[now_size + 1];
            if(!reader->fread(buffer, now_size, now_begin + idx * now_size)) {
				std::cout << "read fail dst" << std::endl;
				++*(int *)NULL;
            }
            row.push_back(new string(buffer));
        }
        return result;
    }

    template<typename Key_t>
    BlockParser<Key_t>::Tuple GetTuple(Key_t key) {
        idx_t single_read_num = this->options->READ_BUFFER_SIZE / key_size;
        size_t single_read_size = single_read_num * key_size;
        int read_times = (key_num - 1) / single_read_num + 1; 
        size_t offset = 0;
        for(int i = 0; i < read_times - 1; i++) {
			char buffer[single_read_size];
			if (!reader->fread(buffer, single_read_size, offset)) {
				std::cout << "read fail dst" << std::endl;
				++*(int *)NULL;
			}
            offset += single_read_size;
            size_t inner_off = 0;
            for(int j = 0; j < single_read_num; j++) {
                Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + inner_off);    
                if(nowkey == key) {
                    return GetTupleWithIdx(Key_t key, i * single_read_num + j);
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
        for(int j = 0; j < tmp_read_num; j++) {
            Key_t nowkey = util::GetDecodeFixed<Key_t>(buffer + inner_off);    
            if(nowkey == key) {
                return GetTupleWithIdx(Key_t key, (read_times - 1) * single_read_num + j);
            }
            inner_off += key_size;
        }
    }
    
    template<typename Key_t>
    BlockParser<Key_t>::Key_t* GetKeyCol() {
        //needs seperately buffering ?
        char* buffer = static_cast<char*>(malloc(key_num * key_size));
        if (!reader->fread(buffer, key_num * key_size, 0)) {
            std::cout << "read fail dst" << std::endl;
            ++*(int *)NULL;
        }
        return static_cast<Key_t*>buffer;
    }

    template<typename Key_t>
    BlockParser<Key_t>::idx_t* GetValCol(int col_id) {
        if(col_id) {
            char* buffer = static_cast<char*>(malloc(key_num * col_size[col_id]));
            if (!reader->fread(buffer, key_num * col_size[col_id], col_data_endpos[col_id - 1])) {
                std::cout << "read fail dst" << std::endl;
                ++*(int *)NULL;
            }
            return static_cast<idx_t*>buffer;
        } else {
            char* buffer = static_cast<char*>(malloc(key_num * col_size[col_id]));
            if (!reader->fread(buffer, key_num * col_size[col_id], key_data_endpos)) {
                std::cout << "read fail dst" << std::endl;
                ++*(int *)NULL;
            }
            return static_cast<idx_t*>buffer;
        }
    }
}

