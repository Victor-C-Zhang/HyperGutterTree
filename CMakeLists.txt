cmake_minimum_required(VERSION 3.15)
include(ExternalProject)
include(FetchContent)

project(GutterTree)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

#Uncomment to enable debug
#set(ENV{DEBUG} ON)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message("Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message("Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
  message("Adding AppleClang compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
else()
  message("${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

message("${CMAKE_MODULE_PATH}")
include("DownloadProject.cmake")

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG "release-1.11.0"

  UPDATE_DISCONNECTED OFF
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
endif()


# Common types and components
FetchContent_Declare(
  GraphZeppelinCommon

  GIT_REPOSITORY  "https://github.com/GraphStreamingProject/GraphZeppelinCommon.git"
  GIT_TAG         "main"

  UPDATE_DISCONNECTED ON
)

FetchContent_MakeAvailable(googletest GraphZeppelinCommon)

add_library(GutterTree
    include/buffering_system.h
    src/gutter_tree.cpp
    include/gutter_tree.h
    src/buffer_control_block.cpp
    include/buffer_control_block.h
    src/buffer_flusher.cpp
    include/buffer_flusher.h
    src/standalone_gutters.cpp
    include/standalone_gutters.h
    src/work_queue.cpp
    include/work_queue.h
    include/types.h)
add_dependencies(GutterTree GraphZeppelinCommon)
target_link_libraries(GutterTree PUBLIC gtest GraphZeppelinCommon)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(GutterTree PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    target_compile_options(GutterTree PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(GutterTree PRIVATE /O2)
  endif()
endif()
# different OSes have different ways of efficiently pre-allocating large files
if (UNIX AND NOT APPLE)
  message("Enabling Fallocate for a linux system")
  target_compile_options(GutterTree PRIVATE -DLINUX_FALLOCATE)
elseif(WIN32)
  message("Using fileapi for Windows")
  target_compile_options(GutterTree PRIVATE -DWINDOWS_FILEAPI)
else()
  message("Using fcntl for an Apple or other unknown system")
  target_compile_options(GutterTree PRIVATE -DPOSIX_FCNTL)
endif ()
set_target_properties(GutterTree PROPERTIES PUBLIC_HEADER 
  "include/gutter_tree.h;include/buffer_control_block.h;include/standalone_gutters.h;include/buffer_flusher.h;include/work_queue.h;include/types.h;include/buffering_system.h"
)

add_executable(buffering_tests
  test/runner.cpp
  test/gutter_tree_test.cpp
  test/standalone_gutters_test.cpp)
target_link_libraries(buffering_tests PRIVATE GutterTree)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(buffering_tests PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    target_compile_options(buffering_tests PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(buffering_tests PRIVATE /O2)
  endif()
endif ()

add_executable(buffering_experiment
  experiment/runner.cpp
  experiment/gutter_tree_exp.cpp
  experiment/standalone_exp.cpp)
target_link_libraries(buffering_experiment PRIVATE GutterTree)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(buffering_experiment PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    target_compile_options(buffering_experiment PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(buffering_experiment PRIVATE /O2)
  endif()
endif ()


#uncomment if manually installing project
#without specifying INSTALL_PREFIX elsewhere
#set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/GutterTree/prefix)

# for use when building as an external project
install(TARGETS GutterTree
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
