#include <bitset>
#include "../compress/ordered_dictionary.h"
#include "../utils/types.h"

namespace BACH {

    template <typename Func, size_t BIT_SIZE>
    void onesiderange(OrderedDictionary* dict, idx_t* data, int count, Func* check, std::bitset<BIT_SIZE>& result){
        
        bool f = check(dict->getString(0));
        int l=0, r=count-1;
        while(l<=r) {
            int mid=(l+r)>>1;
            if (check(dict->getString(mid)) == f) l = mid+1;
            else r = mid-1;
        }
        
        for(int i=0; i<count; i++){
            result[i] = (data[i]<l)^f;
        }
    }

}