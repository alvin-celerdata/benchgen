// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <cctype>
#include <string>

#include "benchgen/benchmark_suite.h"

namespace benchgen {

std::unique_ptr<BenchmarkSuite> MakeTpchBenchmarkSuite();
std::unique_ptr<BenchmarkSuite> MakeTpcdsBenchmarkSuite();
std::unique_ptr<BenchmarkSuite> MakeSsbBenchmarkSuite();

namespace {

std::string ToLower(std::string value) {
  std::transform(
      value.begin(), value.end(), value.begin(),
      [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

}  // namespace

SuiteId SuiteIdFromString(std::string_view value) {
  std::string lowered = ToLower(std::string(value));
  if (lowered == "tpch") {
    return SuiteId::kTpch;
  }
  if (lowered == "tpcds") {
    return SuiteId::kTpcds;
  }
  if (lowered == "ssb") {
    return SuiteId::kSsb;
  }
  return SuiteId::kUnknown;
}

std::string_view SuiteIdToString(SuiteId suite) {
  switch (suite) {
    case SuiteId::kTpch:
      return "tpch";
    case SuiteId::kTpcds:
      return "tpcds";
    case SuiteId::kSsb:
      return "ssb";
    case SuiteId::kUnknown:
      break;
  }
  return "unknown";
}

std::unique_ptr<BenchmarkSuite> MakeBenchmarkSuite(SuiteId suite) {
  switch (suite) {
    case SuiteId::kTpch:
      return MakeTpchBenchmarkSuite();
    case SuiteId::kTpcds:
      return MakeTpcdsBenchmarkSuite();
    case SuiteId::kSsb:
      return MakeSsbBenchmarkSuite();
    case SuiteId::kUnknown:
      break;
  }
  return nullptr;
}

std::unique_ptr<BenchmarkSuite> MakeBenchmarkSuite(std::string_view name) {
  return MakeBenchmarkSuite(SuiteIdFromString(name));
}

}  // namespace benchgen
