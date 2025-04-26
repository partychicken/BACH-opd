cmake_minimum_required (VERSION 3.8)

if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

SET(CMAKE_C_COMPILER "gcc-12")
SET(CMAKE_CXX_COMPILER "g++-12")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

project ("BACH_demo" LANGUAGES CXX)

include_directories(${PROJECT_SOURCE_DIR})

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS src/*.cpp)

add_library(bach SHARED bind/BACH.cpp ${srcs} "src/TupleEntry.cpp")
target_include_directories(bach PUBLIC include)
target_include_directories(bach PUBLIC include/dynamic_bitset/include)
target_compile_features(bach PUBLIC cxx_std_20)

add_subdirectory(include/dynamic_bitset)
target_link_libraries(bach PRIVATE sul::dynamic_bitset)
add_subdirectory(include/folly)
target_link_libraries(bach PRIVATE folly)

set(BUILD_TEST ON)
set(DEBUG ON)

# set(CMAKE_CXX_FLAGS "-O3 ${CMAKE_CXX_FLAGS}")

option(BUILD_UNITTEST "Build unit tests" OFF)
set(BUILD_UNITTEST ON)

if(DEBUG)
	set(CMAKE_CXX_FLAGS " -g -Wall ${CMAKE_CXX_FLAGS}")
	target_link_options(bach PRIVATE -lprofiler -ltcmalloc)
endif()

if(BUILD_TEST)
	include_directories(${PROJECT_SOURCE_DIR}/include)
	add_executable(${PROJECT_NAME} test/test.cpp ${srcs})
	target_link_libraries(${PROJECT_NAME} PRIVATE sul::dynamic_bitset)
	target_link_libraries(${PROJECT_NAME} PRIVATE folly)
	if (CMAKE_VERSION VERSION_GREATER 3.12)
	  set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_STANDARD 20)
	endif()
endif()

if(BUILD_UNITTEST)
	add_subdirectory(${PROJECT_SOURCE_DIR}/test)
endif()