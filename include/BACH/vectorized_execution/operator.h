#pragma once
#include "BACH/compress/ordered_dictionary.h"
#include "BACH/utils/types.h"

namespace BACH {

    template <typename Func>
    void onesiderange(OrderedDictionary* dict, const idx_t* data, int count, Func* check, sul::dynamic_bitset<>& result) {
        
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

    template <typename Func>
    void rangefilter(OrderedDictionary* dict, const idx_t* data, int count, Func* left_bound, Func* right_bound, sul::dynamic_bitset<>& result) {
        
        bool f = left_bound(dict->getString(0));
        int l=0, r=dict->getCount()-1;
        while(l<=r) {
            int mid=(l+r)>>1;
            if (left_bound(dict->getString(mid)) == f) l = mid+1;
            else r = mid-1;
        }
        int LeftBound = l-1;
        
        l=0;r=dict->getCount()-1;//
        while(l<=r) {
            int mid=(l+r)>>1;
            if (right_bound(dict->getString(mid)) == f) l = mid+1;
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

    template<typename Func>
    void RangeFilter(Vector &res, Func* left_bound, Func* right_bound) {
        idx_t cnt = res.GetCount();
        sul::dynamic_bitset<>res_bitmap(cnt);
        rangefilter(res.GetDict(), res.GetData(), cnt, left_bound, right_bound, res_bitmap);
        res.Slice(res_bitmap);
    }

}