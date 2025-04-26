include_directories(../include)
include_directories(../include/catch)

# 添加测试可执行文件
add_executable(tp_tests
    test_tp.cpp
)

# 包含路径，要能访问你数据库的头文件
target_include_directories(tp_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

# 链接你已经生成的数据库库
target_link_libraries(tp_tests PRIVATE bach)

