include_directories(../include)
include_directories(../include/catch)
include_directories(../include/folly)

# ���Ӳ��Կ�ִ���ļ�
add_executable(tp_tests
    test_tp.cpp
)

# ����·����Ҫ�ܷ��������ݿ��ͷ�ļ�
target_include_directories(tp_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

# �������Ѿ����ɵ����ݿ��
target_link_libraries(tp_tests PRIVATE bach-opd folly)

