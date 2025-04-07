#pragma once
#include <set>
#include <string>
#include <vector>
#include <unordered_map>

namespace BACH {

    class OrderedDictionary {
    public:
        // 导入数据并生成字典
        void importData(const std::vector<std::string>& data);

        // 获取字符串对应的非负整数
        int getMapping(const std::string& str) const;

        // 获取非负整数对应的字符串
        std::string getString(int index) const;

        static OrderedDictionary merge(const OrderedDictionary& dict1, const OrderedDictionary& dict2);

    private:
        
        std::unordered_map<std::string, int> stringToIndex;
		// 前边俩落盘就没了，只是中间状态有用，一个用set去重，一个跑一轮数据的时候防止每个数据都采用二分去查
        std::vector<std::string> indexToString;
    };


}