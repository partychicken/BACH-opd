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
        std::set<std::string> uniqueStrings;
        std::unordered_map<std::string, int> stringToIndex;
        std::vector<std::string> indexToString;
    };


}