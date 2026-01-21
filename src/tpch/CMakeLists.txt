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

# TPCH distribution path (fixed to repository resources).
set(_tpch_distribution_dir "${PROJECT_SOURCE_DIR}/resources/tpch/distribution")

if(IS_DIRECTORY "${_tpch_distribution_dir}")
    set(TPCH_DISTRIBUTION_FILE "${_tpch_distribution_dir}/dists.dss")
else()
    set(TPCH_DISTRIBUTION_FILE "${_tpch_distribution_dir}")
endif()
if(NOT EXISTS "${TPCH_DISTRIBUTION_FILE}")
    message(FATAL_ERROR "TPC-H distribution file not found: ${TPCH_DISTRIBUTION_FILE}")
endif()

find_package(Python3 COMPONENTS Interpreter REQUIRED)

set(TPCH_DISTS_EMBEDDED_CC
    "${BENCHGEN_GENERATED_DIR}/tpch_embedded_dist.cc")
file(RELATIVE_PATH TPCH_DISTRIBUTION_LABEL
    "${PROJECT_SOURCE_DIR}" "${TPCH_DISTRIBUTION_FILE}")
if(TPCH_DISTRIBUTION_LABEL MATCHES "^\\.\\.")
    set(TPCH_DISTRIBUTION_LABEL "${TPCH_DISTRIBUTION_FILE}")
endif()
add_custom_command(
    OUTPUT "${TPCH_DISTS_EMBEDDED_CC}"
    COMMAND ${CMAKE_COMMAND} -E make_directory
        "${BENCHGEN_GENERATED_DIR}"
    COMMAND ${Python3_EXECUTABLE}
        "${PROJECT_SOURCE_DIR}/scripts/tpch/generate_dists_embedded.py"
        --input "${TPCH_DISTRIBUTION_FILE}"
        --output "${TPCH_DISTS_EMBEDDED_CC}"
        --label "${TPCH_DISTRIBUTION_LABEL}"
    DEPENDS
        "${TPCH_DISTRIBUTION_FILE}"
        "${PROJECT_SOURCE_DIR}/scripts/tpch/generate_dists_embedded.py"
    COMMENT "Generating embedded dists.dss"
    VERBATIM
)
set_source_files_properties(
    "${TPCH_DISTS_EMBEDDED_CC}" PROPERTIES GENERATED TRUE)
set(TPCH_EMBED_SOURCES "${TPCH_DISTS_EMBEDDED_CC}")

add_library(tpch_gen_obj OBJECT
    distribution/distribution.cc
    distribution/embedded_distribution.cc
    distribution/distribution_provider.cc
    distribution/distribution_source.cc
    distribution/scaling.cc
    utils/context.cc
    utils/random.cc
    utils/text.cc
    utils/utils.cc
    ${TPCH_EMBED_SOURCES}
    tpch_benchmark_suite.cc
    generators/customer_generator.cc
    generators/customer_row_generator.cc
    generators/lineitem_generator.cc
    generators/lineitem_row_generator.cc
    generators/nation_generator.cc
    generators/nation_row_generator.cc
    generators/orders_generator.cc
    generators/orders_row_generator.cc
    generators/part_generator.cc
    generators/part_row_generator.cc
    generators/partsupp_generator.cc
    generators/partsupp_row_generator.cc
    generators/region_generator.cc
    generators/region_row_generator.cc
    generators/supplier_generator.cc
    generators/supplier_row_generator.cc
)

if(APPLE)
    target_compile_definitions(tpch_gen_obj PRIVATE MAC)
else()
    target_compile_definitions(tpch_gen_obj PRIVATE LINUX)
endif()

target_compile_definitions(tpch_gen_obj PRIVATE TPCH)


target_include_directories(tpch_gen_obj
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        "${PROJECT_SOURCE_DIR}/src/tpch"
        "${PROJECT_SOURCE_DIR}/src"
)

target_link_libraries(tpch_gen_obj PUBLIC ${BENCHGEN_ARROW_TARGET})

install(FILES "${TPCH_SCHEMA_OUTPUT}"
    DESTINATION ${CMAKE_INSTALL_DATADIR}/benchgen/schema)
