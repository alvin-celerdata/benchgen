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

add_library(tpcds_gen_obj OBJECT
    tpcds_benchmark_suite.cc
    utils/address.cc
    utils/build_support.cc
    utils/column_streams.cc
    utils/date.cc
    utils/decimal.cc
    utils/join.cc
    utils/null_utils.cc
    utils/permute.cc
    utils/pricing.cc
    utils/random_number_stream.cc
    utils/random_utils.cc
    utils/row_streams.cc
    utils/scd.cc
    utils/table_metadata.cc
    utils/text.cc
    distribution/date_scaling.cc
    distribution/distribution_provider.cc
    distribution/dst_distribution.cc
    distribution/dst_distribution_store.cc
    distribution/dst_distribution_utils.cc
    distribution/embedded_distribution.cc
    distribution/scaling.cc
    distribution/string_values_distribution.cc
    generators/call_center_generator.cc
    generators/call_center_row_generator.cc
    generators/catalog_page_generator.cc
    generators/catalog_page_row_generator.cc
    generators/catalog_returns_generator.cc
    generators/catalog_returns_row_generator.cc
    generators/catalog_sales_generator.cc
    generators/catalog_sales_row_generator.cc
    generators/customer_address_generator.cc
    generators/customer_address_row_generator.cc
    generators/customer_demographics_generator.cc
    generators/customer_demographics_row_generator.cc
    generators/customer_generator.cc
    generators/customer_row_generator.cc
    generators/date_dim_generator.cc
    generators/date_dim_row_generator.cc
    generators/household_demographics_generator.cc
    generators/household_demographics_row_generator.cc
    generators/income_band_generator.cc
    generators/income_band_row_generator.cc
    generators/inventory_generator.cc
    generators/inventory_row_generator.cc
    generators/item_generator.cc
    generators/item_row_generator.cc
    generators/promotion_generator.cc
    generators/promotion_row_generator.cc
    generators/reason_generator.cc
    generators/reason_row_generator.cc
    generators/ship_mode_generator.cc
    generators/ship_mode_row_generator.cc
    generators/store_generator.cc
    generators/store_returns_generator.cc
    generators/store_returns_row_generator.cc
    generators/store_row_generator.cc
    generators/store_sales_generator.cc
    generators/store_sales_row_generator.cc
    generators/time_dim_generator.cc
    generators/time_dim_row_generator.cc
    generators/warehouse_generator.cc
    generators/warehouse_row_generator.cc
    generators/web_page_generator.cc
    generators/web_page_row_generator.cc
    generators/web_returns_generator.cc
    generators/web_returns_row_generator.cc
    generators/web_sales_generator.cc
    generators/web_sales_row_generator.cc
    generators/web_site_generator.cc
    generators/web_site_row_generator.cc
)

set(_tpcds_distribution_dir "${PROJECT_SOURCE_DIR}/resources/tpcds/distribution")

find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_executable(tpcds_gen_idx
    tpcds_gen_idx_main.cc
)
target_compile_definitions(tpcds_gen_idx PRIVATE
    TPCDS_RESOURCE_DISTRIBUTION_DIR=\"${_tpcds_distribution_dir}\")

file(GLOB TPCDS_DISTRIBUTION_DST_FILES "${_tpcds_distribution_dir}/*.dst")
set(TPCDS_GENERATED_IDX "${BENCHGEN_GENERATED_DIR}/tpcds.idx")
add_custom_command(
    OUTPUT "${TPCDS_GENERATED_IDX}"
    COMMAND ${CMAKE_COMMAND} -E make_directory
        "${BENCHGEN_GENERATED_DIR}"
    COMMAND "$<TARGET_FILE:tpcds_gen_idx>"
        --output "${TPCDS_GENERATED_IDX}"
    DEPENDS
        tpcds_gen_idx
        ${TPCDS_DISTRIBUTION_DST_FILES}
    COMMENT "Generating tpcds.idx"
    VERBATIM
)

set(TPCDS_EMBEDDED_DATA_CC
    "${BENCHGEN_GENERATED_DIR}/tpcds_embedded_dist.cc")
add_custom_command(
    OUTPUT "${TPCDS_EMBEDDED_DATA_CC}"
    COMMAND ${CMAKE_COMMAND} -E make_directory
        "${BENCHGEN_GENERATED_DIR}"
    COMMAND "${Python3_EXECUTABLE}"
        "${PROJECT_SOURCE_DIR}/scripts/tpcds/generate_embedded_distribution.py"
        --distribution-dir "${BENCHGEN_GENERATED_DIR}"
        --output "${TPCDS_EMBEDDED_DATA_CC}"
        --files "tpcds.idx"
    DEPENDS
        "${PROJECT_SOURCE_DIR}/scripts/tpcds/generate_embedded_distribution.py"
        "${TPCDS_GENERATED_IDX}"
    COMMENT "Generating embedded distribution data"
    VERBATIM
)

target_sources(tpcds_gen_obj PRIVATE "${TPCDS_EMBEDDED_DATA_CC}")

target_include_directories(tpcds_gen_obj
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src/tpcds
        ${PROJECT_SOURCE_DIR}/src
)

if(_benchmark_use_system_arrow AND _benchmark_arrow_prefixes)
    foreach(prefix IN LISTS _benchmark_arrow_prefixes)
        target_include_directories(tpcds_gen_obj PRIVATE "${prefix}/include")
    endforeach()
endif()

target_link_libraries(tpcds_gen_obj PUBLIC ${BENCHGEN_ARROW_TARGET})

set(_tpcds_executables
    tpcds_gen_idx
)

if(_benchmark_static_stdlib_options)
    foreach(_tpcds_exec IN LISTS _tpcds_executables)
        target_link_options(${_tpcds_exec} PRIVATE ${_benchmark_static_stdlib_options})
    endforeach()
endif()

set(_tpcds_output_targets
    tpcds_gen_idx
)
benchmark_set_output_prefix("${BENCHGEN_OUTPUT_PREFIX}" ${_tpcds_output_targets})

install(FILES "${TPCDS_SCHEMA_OUTPUT}"
    DESTINATION ${CMAKE_INSTALL_DATADIR}/benchgen/schema
)
