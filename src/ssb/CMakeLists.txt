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

set(_ssb_distribution_dir "${PROJECT_SOURCE_DIR}/resources/ssb/distribution")

set(_ssb_distribution_file "${_ssb_distribution_dir}/dists.dss")
if(NOT EXISTS "${_ssb_distribution_file}")
    message(FATAL_ERROR "SSB distribution file not found: ${_ssb_distribution_file}")
endif()
find_package(Python3 COMPONENTS Interpreter REQUIRED)
set(_ssb_embedded_distribution_cc
    "${BENCHGEN_GENERATED_DIR}/ssb_embedded_dist.cc")
add_custom_command(
    OUTPUT "${_ssb_embedded_distribution_cc}"
    COMMAND ${CMAKE_COMMAND} -E make_directory
        "${BENCHGEN_GENERATED_DIR}"
    COMMAND "${Python3_EXECUTABLE}"
        "${PROJECT_SOURCE_DIR}/scripts/ssb/generate_embedded_distribution.py"
        --input "${_ssb_distribution_file}"
        --output "${_ssb_embedded_distribution_cc}"
    DEPENDS
        "${_ssb_distribution_file}"
        "${PROJECT_SOURCE_DIR}/scripts/ssb/generate_embedded_distribution.py"
    COMMENT "Generating embedded distribution data"
    VERBATIM
)
set_source_files_properties(
    "${_ssb_embedded_distribution_cc}" PROPERTIES GENERATED TRUE)
set(SSB_EMBEDDED_DISTRIBUTION_SOURCES "${_ssb_embedded_distribution_cc}")

add_library(ssb_gen_obj OBJECT
    ssb_benchmark_suite.cc
    distribution/distribution.cc
    distribution/distribution_provider.cc
    distribution/distribution_source.cc
    utils/context.cc
    utils/random.cc
    utils/utils.cc
    utils/scaling.cc
    generators/customer_generator.cc
    generators/customer_row_generator.cc
    generators/part_generator.cc
    generators/part_row_generator.cc
    generators/supplier_generator.cc
    generators/supplier_row_generator.cc
    generators/date_generator.cc
    generators/date_row_generator.cc
    generators/lineorder_generator.cc
    generators/lineorder_row_generator.cc
    ${SSB_EMBEDDED_DISTRIBUTION_SOURCES}
)

target_include_directories(ssb_gen_obj
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src/ssb
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(ssb_gen_obj PUBLIC ${BENCHGEN_ARROW_TARGET})


install(FILES "${SSB_SCHEMA_OUTPUT}"
    DESTINATION ${CMAKE_INSTALL_DATADIR}/benchgen/schema)
