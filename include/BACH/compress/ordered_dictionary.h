#pragma once
#include <set>
#include <string>
#include <vector>
#include <unordered_map>

namespace BACH {

    class OrderedDictionary {
    public:
        // 锟斤拷锟斤拷锟斤拷锟捷诧拷锟斤拷锟斤拷锟街碉拷
        OrderedDictionary(){}

        explicit OrderedDictionary(const std::vector<std::string>& data);

        void importData(const std::vector<std::string>& data);

        // 锟斤拷取锟街凤拷锟斤拷锟斤拷应锟侥非革拷锟斤拷锟斤拷
        int getMapping(const std::string& str) const;

        // 锟斤拷取锟角革拷锟斤拷锟斤拷锟斤拷应锟斤拷锟街凤拷锟斤拷
        std::string getString(int index) const;

        static OrderedDictionary merge(const OrderedDictionary& dict1, const OrderedDictionary& dict2);
        
        int getCount() {
            return indexToString.size();
        }

    private:
        
        std::unordered_map<std::string, int> stringToIndex;
		// 前边俩落盘就没了，只是中间状态有用，一个用set去重，一个跑一轮数据的时候防止每个数据都采用二分去查
        std::vector<std::string> indexToString;
    };


}