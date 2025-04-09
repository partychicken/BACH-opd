#include <bitset>
#include "BACH/compress/ordered_dictionary.h"
#include "BACH/utils/types.h"

namespace BACH {

    template <typename Func, size_t BIT_SIZE>
    void onesiderange(OrderedDictionary* dict, idx_t* data, int count, Func* check, std::bitset<BIT_SIZE>& result) {
        
        bool f = check(dict->getString(0));
        int l=0, r=dict->getCount()-1;
        while(l<=r) {
            int mid=(l+r)>>1;
            if (check(dict->getString(mid)) == f) l = mid+1;
            else r = mid-1;
        }
        
        for(int i=0; i<count; i++){
            result[i] = (data[i]<l)^f;
        }
    }

    template <typename Func, size_t BIT_SIZE>
    void rangefilter(OrderedDictionary* dict, idx_t* data, int count, Func* leftbound, Func* rightbound, std::bitset<BIT_SIZE>& result) {
        
        bool f = leftbound(dict->getString(0));
        int l=0, r=dict->getCount()-1;
        while(l<=r) {
            int mid=(l+r)>>1;
            if (leftbound(dict->getString(mid)) == f) l = mid+1;
            else r = mid-1;
        }
        int LeftBound = l-1;
        
        l=0;r=dict->getCount()-1;//
        while(l<=r) {
            int mid=(l+r)>>1;
            if (rightbound(dict->getString(mid)) == f) l = mid+1;
            else r = mid-1;
        }
        int RightBound = l-1;

        if(LeftBound > RightBound) {
            // !!! maybe not swap? need to return empty set
            std :: swap(LeftBound, RightBound);
        }

        for(int i=0; i<count; i++){
            result[i] = (LeftBound<=data[i] && data[i]<=RightBound);
        }
    }

}