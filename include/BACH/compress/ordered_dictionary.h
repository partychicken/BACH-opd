#pragma once
#include <set>
#include <string>
#include <vector>
#include <unordered_map>

namespace BACH {

    class OrderedDictionary {
    public:
        // �������ݲ������ֵ�
        OrderedDictionary(){}

        explicit OrderedDictionary(const std::vector<std::string>& data);

        void importData(const std::vector<std::string>& data);

        // ��ȡ�ַ�����Ӧ�ķǸ�����
        int getMapping(const std::string& str) const;

        // ��ȡ�Ǹ�������Ӧ���ַ���
        std::string getString(int index) const;

        static OrderedDictionary merge(const OrderedDictionary& dict1, const OrderedDictionary& dict2);
        
        int getCount() {
            return indexToString.size();
        }

    private:
        
        std::unordered_map<std::string, int> stringToIndex;
		// ǰ�������̾�û�ˣ�ֻ���м�״̬���ã�һ����setȥ�أ�һ����һ�����ݵ�ʱ���ֹÿ�����ݶ����ö���ȥ��
        std::vector<std::string> indexToString;
    };


}