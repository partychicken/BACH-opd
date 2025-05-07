include_directories(../include)
include_directories(../include/catch)

# ���Ӳ��Կ�ִ���ļ�
add_executable(ap_tests
    test_ap.cpp
)

# ����·����Ҫ�ܷ��������ݿ��ͷ�ļ�
target_include_directories(ap_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

# 链接你已经生成的数据库
target_link_libraries(ap_tests PRIVATE bach-opd folly)

