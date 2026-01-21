# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
project(benchmark_gen LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)

if(MSVC)
    add_compile_options($<$<CONFIG:Release>:/O2>)
else()
    add_compile_options($<$<CONFIG:Release>:-O2>)
endif()

# Shared dependency options
set(BENCHGEN_ARROW_PREFIX "" CACHE STRING "Arrow installation prefix (leave empty for auto-detect)")
set(BENCHGEN_GTEST_PREFIX "" CACHE STRING "GTest installation prefix (leave empty for auto-detect)")

# Shared static stdlib options
set(_benchmark_static_stdlib_predefined FALSE)
if(DEFINED BENCHGEN_STATIC_STDLIB)
    set(_benchmark_static_stdlib_predefined TRUE)
endif()
option(BENCHGEN_STATIC_STDLIB "Statically link the C++ standard library into executables" ON)

# Shared test options
set(_benchmark_enable_tests_predefined FALSE)
if(DEFINED BENCHGEN_ENABLE_TESTS)
    set(_benchmark_enable_tests_predefined TRUE)
endif()
option(BENCHGEN_ENABLE_TESTS "Build tests" OFF)

# Shared output naming options
set(BENCHGEN_OUTPUT_PREFIX "" CACHE STRING "Prefix for library and CLI output names")

# Arrow dependency
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake_modules")
include(BenchmarkArrow)

# GoogleTest dependency
set(_benchmark_enable_tests "${BENCHGEN_ENABLE_TESTS}")

if(_benchmark_enable_tests)
    set(_benchmark_gtest_prefixes "")
    if(BENCHGEN_GTEST_PREFIX)
        set(_benchmark_gtest_prefixes "${BENCHGEN_GTEST_PREFIX}")
    endif()

    foreach(prefix IN LISTS _benchmark_gtest_prefixes)
        list(APPEND CMAKE_PREFIX_PATH "${prefix}")
    endforeach()

    find_package(GTest QUIET)
    if(NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG release-1.10.0
        )
        FetchContent_MakeAvailable(googletest)
    endif()

    enable_testing()
endif()

# Static stdlib options
set(_benchmark_static_stdlib_options "")
if(BENCHGEN_STATIC_STDLIB)
    if(APPLE)
        message(WARNING
            "Static standard library linking is not supported on Apple toolchains; "
            "skipping static stdlib flags.")
    else()
        set(_benchmark_static_stdlib_impl "")
        if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set(_benchmark_static_stdlib_impl "libstdc++")
        elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            set(_benchmark_static_stdlib_impl "libstdc++")
        endif()

        if(_benchmark_static_stdlib_impl STREQUAL "libc++")
            if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
                list(APPEND _benchmark_static_stdlib_options -static-libc++)
            else()
                message(WARNING
                    "Static libc++ linking requires Clang; skipping libc++ static link.")
            endif()
        elseif(_benchmark_static_stdlib_impl STREQUAL "libstdc++")
            list(APPEND _benchmark_static_stdlib_options -static-libstdc++)
        endif()

        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            list(APPEND _benchmark_static_stdlib_options -static-libgcc)
        endif()
    endif()
endif()

# Build outputs
set(BENCHGEN_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(BENCHGEN_GENERATED_DIR "${BENCHGEN_BINARY_DIR}/generated")
set(TPCH_SCHEMA_OUTPUT "${BENCHGEN_BINARY_DIR}/tpch_schema.json")
set(TPCDS_SCHEMA_OUTPUT "${BENCHGEN_BINARY_DIR}/tpcds_schema.json")
set(SSB_SCHEMA_OUTPUT "${BENCHGEN_BINARY_DIR}/ssb_schema.json")

add_subdirectory(src)
add_subdirectory(tests)
