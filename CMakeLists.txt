cmake_minimum_required(VERSION 3.17)
project(FastBufferTree)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message("Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message("Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
else()
  message("${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

message("${CMAKE_MODULE_PATH}")
include("DownloadProject.cmake")

#Find or download GTest
find_package(GTest CONFIG)
if(NOT GTest_FOUND)
  if(NOT AUTODOWNLOAD)
    message(FATAL_ERROR "GTest config not found.\n"
      "Set GTest_DIR to a directory containing one of \n"
      "  GTestConfig.cmake\n"
      "  gtest-config.cmake\n"
      "or use -DAUTODOWNLOAD:BOOL=ON to automatically download")
  else()
    #Download googletest
    download_project(
      PROJ googletest
      GIT_REPOSITORY "https://github.com/google/googletest.git"
      GIT_TAG "release-1.10.0"
    )

    #As specified in the googletest docs, need to set
    #gtest_force_shared_crt for windows visual studio.
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
      set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    endif()

    add_subdirectory(
      "${googletest_SOURCE_DIR}"
      "${googletest_BINARY_DIR}"
      EXCLUDE_FROM_ALL
    )

    #Add aliases to targets
    add_library(GTest::gtest ALIAS gtest)
    add_library(GTest::gtest_main ALIAS gtest_main)
    add_library(GTest::gmock ALIAS gmock)
    add_library(GTest::gmock_main ALIAS gmock_main)
  endif()
endif()

add_library(FastBufferTree
    buffer_tree.cpp
    include/buffer_tree.h
    buffer_control_block.cpp
    include/buffer_control_block.h
    include/update.h sketch_write_manager.cpp include/sketch_write_manager.h include/sketch.h)
target_link_libraries(FastBufferTree PRIVATE GTest::gtest)
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  target_compile_options(FastBufferTree PRIVATE -O3)
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  target_compile_options(FastBufferTree PRIVATE /O2)
endif()

add_executable(test
  test/runner.cpp)
target_link_libraries(test PRIVATE GTest::gtest)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(test PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    target_compile_options(test PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(test PRIVATE /O2)
  endif()
endif ()
