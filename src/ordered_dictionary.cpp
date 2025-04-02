#include "BACH/compress/ordered_dictionary.h"

namespace BACH
{
    void OrderedDictionary::importData(const std::vector<std::string>& data) {
        // 使用 set 进行去重
        uniqueStrings.insert(data.begin(), data.end());

        // 清空现有的映射
        stringToIndex.clear();
        indexToString.clear();

        // 对 set 内的数据进行排序并生成映射
        int index = 0;
        for (const auto& str : uniqueStrings) {
            stringToIndex[str] = index;
            indexToString.push_back(str);
            ++index;
        }
    }

    int OrderedDictionary::getMapping(const std::string& str) const {
        auto it = stringToIndex.find(str);
        if (it != stringToIndex.end()) {
            return it->second;
        }
        return -1; // 如果字符串不在字典中，返回 -1
    }

    std::string OrderedDictionary::getString(int index) const {
        if (index >= 0 && index < static_cast<int>(indexToString.size())) {
            return indexToString[index];
        }
        return ""; // 如果索引无效，返回空字符串
    }


    OrderedDictionary OrderedDictionary::merge(const OrderedDictionary& dict1, const OrderedDictionary& dict2) {
        OrderedDictionary mergedDict;

        auto it1 = dict1.uniqueStrings.begin();
        auto it2 = dict2.uniqueStrings.begin();

        while (it1 != dict1.uniqueStrings.end() && it2 != dict2.uniqueStrings.end()) {
            if (*it1 < *it2) {
                mergedDict.uniqueStrings.insert(*it1);
                ++it1;
            }
            else if (*it2 < *it1) {
                mergedDict.uniqueStrings.insert(*it2);
                ++it2;
            }
            else {
                mergedDict.uniqueStrings.insert(*it1);
                ++it1;
                ++it2;
            }
        }

        // 插入剩余的元素
        while (it1 != dict1.uniqueStrings.end()) {
            mergedDict.uniqueStrings.insert(*it1);
            ++it1;
        }

        while (it2 != dict2.uniqueStrings.end()) {
            mergedDict.uniqueStrings.insert(*it2);
            ++it2;
        }

        // 生成映射
        int index = 0;
        for (const auto& str : mergedDict.uniqueStrings) {
            mergedDict.stringToIndex[str] = index;
            mergedDict.indexToString.push_back(str);
            ++index;
        }

        return mergedDict;
    }
}// namespace BACH