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

add_executable(tpcds_gen_tests
    customer_generator_test.cc
    generator_start_row_test.cc
    row_generator_skip_rows_test.cc
    utils/random_number_stream_test.cc
    md5.cc
)

target_link_libraries(tpcds_gen_tests PRIVATE benchgen GTest::gtest_main)

target_include_directories(tpcds_gen_tests PRIVATE
    "${PROJECT_SOURCE_DIR}/src/tpcds"
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

if(_benchmark_use_system_gtest AND _benchmark_gtest_prefixes)
    foreach(prefix IN LISTS _benchmark_gtest_prefixes)
        target_include_directories(tpcds_gen_tests PRIVATE "${prefix}/include")
    endforeach()
endif()

if(_benchmark_static_stdlib_options)
    target_link_options(tpcds_gen_tests PRIVATE ${_benchmark_static_stdlib_options})
endif()

add_test(NAME tpcds_gen_tests COMMAND tpcds_gen_tests)
