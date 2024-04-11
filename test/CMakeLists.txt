cmake_minimum_required(VERSION 3.27)
project(test)

set(CMAKE_CXX_STANDARD 17)

include_directories(.)

add_executable(test
        kvstore.cc
        kvstore.h
        kvstore_api.h
        main.cc
        skiplist.cc
        skiplist.h
)
