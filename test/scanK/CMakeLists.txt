include_directories(../include)
include_directories(../include/catch)

# ���Ӳ��Կ�ִ���ļ�
add_executable(scanK
    scanK.cpp
)

# ����·����Ҫ�ܷ��������ݿ��ͷ�ļ�
target_include_directories(scanK PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

# �������Ѿ����ɵ����ݿ��
target_link_libraries(scanK PRIVATE bach-opd folly profiler tcmalloc)
