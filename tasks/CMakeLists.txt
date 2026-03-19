cmake_minimum_required(VERSION 3.16)

project(futez_hw3 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(futez_hw3 futez_hw3.cpp)
target_link_libraries(futez_hw3 PRIVATE Threads::Threads)
