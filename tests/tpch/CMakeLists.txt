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

add_executable(tpch_gen_tests
    skip_rows_test.cc
    row_count_test.cc
)

target_link_libraries(tpch_gen_tests PRIVATE GTest::gtest_main benchgen)
target_include_directories(tpch_gen_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/src/tpch
)

if(_benchmark_static_stdlib_options)
    target_link_options(tpch_gen_tests PRIVATE ${_benchmark_static_stdlib_options})
endif()

add_test(NAME tpch_gen_tests COMMAND tpch_gen_tests)
