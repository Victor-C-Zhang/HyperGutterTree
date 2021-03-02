cmake_minimum_required(VERSION 3.17)
project(FastBufferTree)

set(CMAKE_CXX_STANDARD 11)

add_library(FastBufferTree
    buffer_tree.cpp
    include/buffer_tree.h
    buffer_control_block.cpp
    include/buffer_control_block.h
    include/update.h sketch_write_manager.cpp include/sketch_write_manager.h include/sketch.h)