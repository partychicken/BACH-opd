# CMakeList.txt: BACH 的 CMake 项目，在此处包括源代码并定义
# 项目特定的逻辑。
#
cmake_minimum_required (VERSION 3.8)

# 如果支持，请为 MSVC 编译器启用热重载。
if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

project ("BACH_demo" LANGUAGES CXX)

include_directories(${PROJECT_SOURCE_DIR})

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS src/*.cpp)

#add_library(bachlib STATIC ${srcs})
#set_property(TARGET bachlib PROPERTY POSITION_INDEPENDENT_CODE ON)
#target_include_directories(bachlib PUBLIC include)

add_library(bach SHARED bind/BACH.cpp ${srcs})
target_include_directories(bach PUBLIC include)

#target_compile_features(bachlib PUBLIC cxx_std_17)
target_compile_features(bach PUBLIC cxx_std_20)

#add_subdirectory(include/dynamic_bitset)
add_subdirectory(include/CPMA)
#target_link_libraries(bachlib PRIVATE sul::dynamic_bitset)
#target_link_libraries(bach PRIVATE sul::dynamic_bitset)
target_link_libraries(bach PRIVATE CPMA)

set(BUILD_TEST OFF)
set(DEBUG ON)

set(CMAKE_CXX_FLAGS "-O3 -DNDEBUG ${CMAKE_CXX_FLAGS}")
if(DEBUG)
	set(CMAKE_CXX_FLAGS "-g -Wall ${CMAKE_CXX_FLAGS}")
	target_link_options(bach PRIVATE -lprofiler -ltcmalloc)
endif()

if(BUILD_TEST)
	include_directories(${PROJECT_SOURCE_DIR}/include)
	add_executable(${PROJECT_NAME} test/BACH.cpp ${srcs})
	target_link_libraries(${PROJECT_NAME} PRIVATE sul::dynamic_bitset)
	target_link_libraries(${PROJECT_NAME} PRIVATE CPMA)
	if (CMAKE_VERSION VERSION_GREATER 3.12)
	  set_property(TARGET ${PROJECT_NAME} PROPERTY CXX_STANDARD 20)
	endif()
endif()
