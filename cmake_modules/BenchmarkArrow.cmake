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

# Arrow dependency
set(_benchmark_arrow_prefixes "")
if(BENCHGEN_ARROW_PREFIX)
    set(_benchmark_arrow_prefixes "${BENCHGEN_ARROW_PREFIX}")
endif()
foreach(prefix IN LISTS _benchmark_arrow_prefixes)
    list(APPEND CMAKE_MODULE_PATH "${prefix}/lib/cmake/arrow")
    list(APPEND CMAKE_MODULE_PATH "${prefix}/lib/cmake/Arrow")
    list(APPEND CMAKE_MODULE_PATH "${prefix}/lib64/cmake/arrow")
    list(APPEND CMAKE_MODULE_PATH "${prefix}/lib64/cmake/Arrow")
    list(APPEND CMAKE_PREFIX_PATH "${prefix}")
    list(APPEND CMAKE_PREFIX_PATH "${prefix}/lib64/cmake/arrow")
    list(APPEND CMAKE_PREFIX_PATH "${prefix}/lib64/cmake/Arrow")
endforeach()

function(benchmark_add_arrow_from_prefix prefix)
    if(NOT prefix)
        return()
    endif()
    if(TARGET Arrow::arrow_shared OR TARGET Arrow::arrow_static OR TARGET Arrow::arrow)
        return()
    endif()

    set(_benchmark_arrow_lib_dirs "${prefix}/lib64" "${prefix}/lib")
    find_library(_benchmark_arrow_lib
        NAMES arrow arrow_static arrow_shared
        PATHS ${_benchmark_arrow_lib_dirs}
        NO_DEFAULT_PATH)
    if(NOT _benchmark_arrow_lib)
        return()
    endif()

    add_library(Arrow::arrow_static STATIC IMPORTED)
    set_target_properties(Arrow::arrow_static PROPERTIES
        IMPORTED_LOCATION "${_benchmark_arrow_lib}"
        INTERFACE_INCLUDE_DIRECTORIES "${prefix}/include")

    set(_benchmark_arrow_deps "")
    find_library(_benchmark_arrow_bundled_lib
        NAMES arrow_bundled_dependencies
        PATHS ${_benchmark_arrow_lib_dirs}
        NO_DEFAULT_PATH)
    if(_benchmark_arrow_bundled_lib)
        list(APPEND _benchmark_arrow_deps "${_benchmark_arrow_bundled_lib}")
    endif()

    foreach(name IN ITEMS brotlienc brotlidec brotlicommon snappy zstd)
        find_library(_benchmark_arrow_dep_${name}
            NAMES ${name}
            PATHS ${_benchmark_arrow_lib_dirs}
            NO_DEFAULT_PATH)
        if(_benchmark_arrow_dep_${name})
            list(APPEND _benchmark_arrow_deps "${_benchmark_arrow_dep_${name}}")
        endif()
    endforeach()

    foreach(name IN ITEMS lz4 z ssl crypto)
        find_library(_benchmark_arrow_sys_${name}
            NAMES ${name}
            PATHS /usr/lib/aarch64-linux-gnu /usr/lib /lib /lib64)
        if(_benchmark_arrow_sys_${name})
            list(APPEND _benchmark_arrow_deps "${_benchmark_arrow_sys_${name}}")
        endif()
    endforeach()

    find_package(Threads QUIET)
    if(Threads_FOUND)
        list(APPEND _benchmark_arrow_deps Threads::Threads)
    endif()
    if(UNIX AND NOT APPLE)
        list(APPEND _benchmark_arrow_deps dl rt)
    endif()

    if(_benchmark_arrow_deps)
        set_target_properties(Arrow::arrow_static PROPERTIES
            INTERFACE_LINK_LIBRARIES "${_benchmark_arrow_deps}")
    endif()
    set(Arrow_FOUND TRUE PARENT_SCOPE)
endfunction()

find_package(Protobuf QUIET)
if(TARGET Protobuf::libprotobuf AND NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf ALIAS Protobuf::libprotobuf)
endif()

find_package(Arrow QUIET)
if(NOT Arrow_FOUND AND BENCHGEN_ARROW_PREFIX)
    benchmark_add_arrow_from_prefix("${BENCHGEN_ARROW_PREFIX}")
endif()

if(NOT Arrow_FOUND AND NOT TARGET Arrow::arrow_shared AND NOT TARGET Arrow::arrow_static
   AND NOT TARGET Arrow::arrow AND NOT TARGET arrow::arrow AND NOT TARGET arrow_shared
   AND NOT TARGET arrow_static)
    include(FetchContent)
    FetchContent_Declare(arrow
        GIT_REPOSITORY https://github.com/apache/arrow.git
        GIT_TAG apache-arrow-19.0.1
    )
    FetchContent_MakeAvailable(arrow)
endif()

set(BENCHGEN_ARROW_TARGET "")
if(TARGET Arrow::arrow_shared)
    set(BENCHGEN_ARROW_TARGET Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
    set(BENCHGEN_ARROW_TARGET Arrow::arrow_static)
elseif(TARGET Arrow::arrow)
    set(BENCHGEN_ARROW_TARGET Arrow::arrow)
elseif(TARGET arrow::arrow)
    set(BENCHGEN_ARROW_TARGET arrow::arrow)
elseif(TARGET arrow_shared)
    set(BENCHGEN_ARROW_TARGET arrow_shared)
elseif(TARGET arrow_static)
    set(BENCHGEN_ARROW_TARGET arrow_static)
else()
    message(FATAL_ERROR
        "No Arrow CMake target found. Tried: Arrow::arrow_shared, Arrow::arrow_static, "
        "Arrow::arrow, arrow::arrow, arrow_shared, arrow_static")
endif()
