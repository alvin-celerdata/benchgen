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

add_executable(ssb_gen_tests
    row_generator_skip_rows_test.cc
)

set_target_properties(ssb_gen_tests PROPERTIES OUTPUT_NAME "${BENCHGEN_OUTPUT_PREFIX}ssb_gen_tests")

target_link_libraries(ssb_gen_tests PRIVATE benchgen GTest::gtest GTest::gtest_main)

target_include_directories(ssb_gen_tests PRIVATE
    "${PROJECT_SOURCE_DIR}/src/ssb"
)

if(_benchmark_use_system_gtest AND _benchmark_gtest_prefixes)
    foreach(prefix IN LISTS _benchmark_gtest_prefixes)
        target_include_directories(ssb_gen_tests PRIVATE "${prefix}/include")
    endforeach()
endif()

if(_benchmark_static_stdlib_options)
    target_link_options(ssb_gen_tests PRIVATE ${_benchmark_static_stdlib_options})
endif()

add_test(NAME ssb_gen_tests COMMAND ssb_gen_tests)
