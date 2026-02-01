cmake_minimum_required(VERSION 3.10)
project(tpch_query5 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(tpch_query5 src/main.cpp src/query5.cpp)

target_include_directories(tpch_query5 PRIVATE include)

find_package(Threads REQUIRED)
target_link_libraries(tpch_query5 PRIVATE Threads::Threads)