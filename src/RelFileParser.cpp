#include "BACH/sstable/RelFileParser.h"
#include "BACH/db/DB.h"

namespace BACH {
    template<typename Key_t>
    RelFileParser<Key_t>::RelFileParser(std::shared_ptr<FileReader> _fileReader,
               std::shared_ptr<Options> _options, size_t _file_size) :
               reader(_fileReader), options(_options), file_size(_file_size) {
        size_t header_size = 2 * sizeof(key_t) + sizeof(size_t) + 2 * sizeof(idx_t);
        char infobuf[header_size];
        if(!reader->rread(infobuf, header_size, header_size)) {
            std::cout << "read fail begin" << std::endl;
            ++*(int *)NULL;
        }
        util::DecodeFixed(infobuf, key_min);
        util::DecodeFixed(infobuf + sizeof(Key_t), key_max);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2, col_num);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(idx_t), block_count);
        util::DecodeFixed(infobuf + sizeof(Key_t) * 2 + sizeof(idx_t) * 2, block_meta_begin_pos);


    }

}