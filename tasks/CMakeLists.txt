
cmake_minimum_required(VERSION 3.16)

project(ipc_hw4 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(ipc_hw4 ipc_hw4.cpp)
target_link_libraries(ipc_hw4 PRIVATE Threads::Threads)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(ipc_hw4 PRIVATE rt)
endif()
