include_directories(../include)
include_directories(../include/catch)

# ���Ӳ��Կ�ִ���ļ�
add_executable(scanK_tests
        scanK.cpp
)

# ����·����Ҫ�ܷ��������ݿ��ͷ�ļ�
target_include_directories(scanK_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

# �������Ѿ����ɵ����ݿ��
target_link_libraries(scanK_tests PRIVATE bach-opd folly)

