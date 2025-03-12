#pragma once
#include <string>
#include <assert.h>
#include "../utils/types.h"

namespace BACH {

template<typename T>
class Dict {
public:
    Dict(){}
    Dict(idx_t size) {
        DictArray = static_cast<T*>(malloc(size * sizeof(T)));
        IndexArray = static_cast<idx_t*>(malloc(size * sizeof(idx_t)));
        dict_size = size;
    }
    Dict(idx_t size, T* dict_data_ptr, idx_t* idx_data_ptr) {
        DictArray = dict_data_ptr;
        IndexArray = idx_data_ptr;
        dict_size = size;
    }
    ~Dict() {
        if(DictArray != nullptr) {
            free(DictArray);
        }
        if(IndexArray != nullptr) {
            free(IndexArray);
        }
    }
    T GetDict(idx_t index) { //unsafe
        assert(index < dict_size);
        return DictArray[index];
    }

private:
    T* DictArray;
    idx_t* IndexArray;
    bool is_ordered;
    idx_t dict_size;
};

}
