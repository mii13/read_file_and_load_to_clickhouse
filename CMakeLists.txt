cmake_minimum_required(VERSION 3.17)
project(clickhouse_loader)

set(CMAKE_CXX_STANDARD 20)
include_directories(clickhouse-cpp)
set(SOURCE_EXE main.cpp sensor_map.h queue.cpp sensor_map.cpp)

add_subdirectory(clickhouse-cpp)
add_executable(clickhouse_loader ${SOURCE_EXE})
target_link_libraries(clickhouse_loader clickhouse-cpp-lib pthread)

