#include <bitset>
#include "../compress/ordered_dictionary.h"

namespace BACH {

    template <typename Func>
    void onesiderange(ordereddictionary* dict, idx_t* data, int count, Func* check, bitset<BIT_SIZE>& result){
        
        bool f = check(dict->indexToString[0]);
        int l=0, r=count-1;
        while(l<=r) {
            int mid=(l+r)>>1;
            if (check(dict->indexToString[mid]) == f) l = mid+1;
            else r = mid-1;
        }
        
        for(int i=0; i<count; i++){
            result[i] = (data[i]<l)^f;
        }
    }

}