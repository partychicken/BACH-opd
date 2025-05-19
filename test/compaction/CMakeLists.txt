include_directories(../include)
include_directories(../include/catch)

# ���Ӳ��Կ�ִ���ļ�
add_executable(compaction_tests
    test_compaction.cpp
)

# ����·����Ҫ�ܷ��������ݿ��ͷ�ļ�
target_include_directories(compaction_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

# �������Ѿ����ɵ����ݿ��
target_link_libraries(compaction_tests PRIVATE bach-opd folly profiler tcmalloc)

