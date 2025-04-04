#include "BACH/compress/ordered_dictionary.h"

namespace BACH
{
    OrderedDictionary::OrderedDictionary(const std::vector<std::string>& data) {
       uniqueStrings.insert(data.begin(), data.end());
    }

    void OrderedDictionary::importData(const std::vector<std::string>& data) {
        // ʹ�� set ����ȥ��
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

        // ����ʣ���Ԫ��
        while (it1 != dict1.uniqueStrings.end()) {
            mergedDict.uniqueStrings.insert(*it1);
            ++it1;
        }

        while (it2 != dict2.uniqueStrings.end()) {
            mergedDict.uniqueStrings.insert(*it2);
            ++it2;
        }

        // ����ӳ��
        int index = 0;
        for (const auto& str : mergedDict.uniqueStrings) {
            mergedDict.stringToIndex[str] = index;
            mergedDict.indexToString.push_back(str);
            ++index;
        }

        return mergedDict;
    }
}// namespace BACH