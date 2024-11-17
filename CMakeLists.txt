cmake_minimum_required (VERSION 3.8)

if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

SET(CMAKE_C_COMPILER "gcc-12")
SET(CMAKE_CXX_COMPILER "g++-12")

project ("BACH_demo" LANGUAGES CXX)

include_directories(${PROJECT_SOURCE_DIR})

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS src/*.cpp)

add_library(bach SHARED bind/BACH.cpp ${srcs})
target_include_directories(bach PUBLIC include)
target_compile_features(bach PUBLIC cxx_std_20)

add_subdirectory(include/dynamic_bitset)
target_link_libraries(bach PRIVATE sul::dynamic_bitset)

set(skiplist_src include/skiplist/src/skiplist.cc)
#set(skiplist_header include/skiplist/*.h)
add_library(skiplist_lib ${skiplist_src})
target_include_directories(skiplist_lib PUBLIC include/skiplist/include)
target_link_libraries(bach PRIVATE skiplist_lib)

set(BUILD_TEST OFF)
set(DEBUG ON)

set(CMAKE_CXX_FLAGS "-O3 ${CMAKE_CXX_FLAGS}")

if(DEBUG)
	set(CMAKE_CXX_FLAGS " -g -Wall ${CMAKE_CXX_FLAGS}")
	target_link_options(bach PRIVATE -lprofiler -ltcmalloc)
endif()

if(BUILD_TEST)
	include_directories(${PROJECT_SOURCE_DIR}/include)
	add_executable(${PROJECT_NAME} test/test.cpp ${srcs})
	target_link_libraries(${PROJECT_NAME} PRIVATE sul::dynamic_bitset)
	if (CMAKE_VERSION VERSION_GREATER 3.12)
	  set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_STANDARD 20)
	endif()
endif()
