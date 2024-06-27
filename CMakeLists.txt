# CMakeList.txt: BACH 的 CMake 项目，在此处包括源代码并定义
# 项目特定的逻辑。
#
cmake_minimum_required (VERSION 3.8)

if (POLICY CMP0074)
	cmake_policy(SET CMP0074 NEW)
endif()


project ("BACH_demo" LANGUAGES CXX)

include_directories(${PROJECT_SOURCE_DIR})

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS src/*.cpp)

add_library(bach SHARED bind/BACH.cpp ${srcs})
target_include_directories(bach PUBLIC include)
target_compile_features(bach PUBLIC cxx_std_20)

find_package(TBB REQUIRED)
add_subdirectory(include/CPMA)
target_link_libraries(bach PRIVATE TBB::tbb)
target_link_libraries(bach PRIVATE CPMA)
#add_subdirectory(include/dynamic_bitset)
#target_link_libraries(bach PRIVATE sul::dynamic_bitset)

set(BUILD_TEST ON)
set(DEBUG ON)

set(CMAKE_CXX_FLAGS "-O3 -DNDEBUG ${CMAKE_CXX_FLAGS}")

if(DEBUG)
	set(CMAKE_CXX_FLAGS "-g -Wall ${CMAKE_CXX_FLAGS}")
	target_link_options(bach PRIVATE -lprofiler -ltcmalloc)
endif()

if(BUILD_TEST)
	include_directories(${PROJECT_SOURCE_DIR}/include)
	add_executable(test test/test.cpp ${srcs})
	target_link_libraries(test PRIVATE CPMA)
	#target_link_libraries(${PROJECT_NAME} PRIVATE sul::dynamic_bitset)
	if (CMAKE_VERSION VERSION_GREATER 3.12)
	  set_property(TARGET test PROPERTY CXX_STANDARD 20)
	endif()
endif()
