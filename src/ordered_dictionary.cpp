#include "BACH/compress/ordered_dictionary.h"

namespace BACH
{
    void OrderedDictionary::importData(const std::vector<std::string>& data) {
        // ʹ�� set ����ȥ��

        std::set<std::string> uniqueStrings;

        uniqueStrings.insert(data.begin(), data.end());

        // ������е�ӳ��
        stringToIndex.clear();
        indexToString.clear();

        // �� set �ڵ����ݽ�����������ӳ��
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
        return -1; // ����ַ��������ֵ��У����� -1
    }

    std::string OrderedDictionary::getString(int index) const {
        if (index >= 0 && index < static_cast<int>(indexToString.size())) {
            return indexToString[index];
        }
        return ""; // ���������Ч�����ؿ��ַ���
    }


    OrderedDictionary OrderedDictionary::merge(const OrderedDictionary& dict1, const OrderedDictionary& dict2) {
        OrderedDictionary mergedDict;

        auto it1 = dict1.indexToString.begin();
        auto it2 = dict2.indexToString.begin();

		mergedDict.stringToIndex.reserve(dict1.stringToIndex.size() + dict2.stringToIndex.size());

        while (it1 != dict1.indexToString.end() && it2 != dict2.indexToString.end()) {
            if (*it1 < *it2) {
                mergedDict.indexToString.push_back(*it1);
                ++it1;
            }
            else if (*it2 < *it1) {
                mergedDict.indexToString.push_back(*it2);
                ++it2;
            }
            else {
                mergedDict.indexToString.push_back(*it1);
                ++it1;
                ++it2;
            }
        }

        // ����ʣ���Ԫ��
        while (it1 != dict1.indexToString.end()) {
            mergedDict.indexToString.push_back(*it1);
            ++it1;
        }

        while (it2 != dict2.indexToString.end()) {
            mergedDict.indexToString.push_back(*it2);
            ++it2;
        }

        return mergedDict;
    }
}// namespace BACH