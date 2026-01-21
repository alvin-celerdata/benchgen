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

function(benchmark_set_output_prefix prefix)
    foreach(target IN LISTS ARGN)
        get_target_property(output_name ${target} OUTPUT_NAME)
        if(NOT output_name OR output_name STREQUAL "output_name-NOTFOUND")
            set(output_name "${target}")
        endif()
        set_target_properties(${target}
            PROPERTIES OUTPUT_NAME "${prefix}${output_name}"
        )
    endforeach()
endfunction()

add_subdirectory(util)
add_subdirectory(tpcds)
add_subdirectory(tpch)
add_subdirectory(ssb)

add_library(benchgen STATIC)
target_sources(benchgen PRIVATE
    $<TARGET_OBJECTS:benchgen_util_obj>
    $<TARGET_OBJECTS:tpch_gen_obj>
    $<TARGET_OBJECTS:tpcds_gen_obj>
    $<TARGET_OBJECTS:ssb_gen_obj>
)
target_link_libraries(benchgen PUBLIC ${BENCHGEN_ARROW_TARGET})
target_include_directories(benchgen
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

add_executable(gen_schema
    gen_schema_main.cc
)

target_link_libraries(gen_schema PRIVATE benchgen)

add_custom_command(
    OUTPUT "${TPCH_SCHEMA_OUTPUT}"
    COMMAND "$<TARGET_FILE:gen_schema>"
        --benchmark tpch
        --output "${TPCH_SCHEMA_OUTPUT}"
    DEPENDS gen_schema
    COMMENT "Generating TPC-H Arrow schema"
    VERBATIM
)
add_custom_target(tpch_schema_file ALL DEPENDS "${TPCH_SCHEMA_OUTPUT}")

add_custom_command(
    OUTPUT "${TPCDS_SCHEMA_OUTPUT}"
    COMMAND "$<TARGET_FILE:gen_schema>"
        --benchmark tpcds
        --output "${TPCDS_SCHEMA_OUTPUT}"
    DEPENDS gen_schema
    COMMENT "Generating TPC-DS Arrow schema"
    VERBATIM
)
add_custom_target(tpcds_schema_file ALL DEPENDS "${TPCDS_SCHEMA_OUTPUT}")

add_custom_command(
    OUTPUT "${SSB_SCHEMA_OUTPUT}"
    COMMAND "$<TARGET_FILE:gen_schema>"
        --benchmark ssb
        --output "${SSB_SCHEMA_OUTPUT}"
    DEPENDS gen_schema
    COMMENT "Generating SSB Arrow schema"
    VERBATIM
)
add_custom_target(ssb_schema_file ALL DEPENDS "${SSB_SCHEMA_OUTPUT}")

add_executable(gen_table
    gen_table_main.cc
)

target_link_libraries(gen_table PRIVATE benchgen)

target_include_directories(gen_table PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/tpch"
    "${CMAKE_CURRENT_SOURCE_DIR}/tpcds"
    "${CMAKE_CURRENT_SOURCE_DIR}/ssb"
)

set_target_properties(gen_table PROPERTIES OUTPUT_NAME "benchgen")

set(_benchgen_output_targets
    benchgen
    gen_schema
    gen_table
)
benchmark_set_output_prefix("${BENCHGEN_OUTPUT_PREFIX}" ${_benchgen_output_targets})

if(_benchmark_static_stdlib_options)
    target_link_options(gen_schema PRIVATE ${_benchmark_static_stdlib_options})
    target_link_options(gen_table PRIVATE ${_benchmark_static_stdlib_options})
endif()

install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS benchgen
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(TARGETS gen_table
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
