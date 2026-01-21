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

#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "benchgen/benchmark_suite.h"
#include "benchgen/generator_options.h"
#include "common/schema_cli.h"

namespace {

void PrintUsage(const char* argv0) {
  std::cerr << "Usage: " << argv0
            << " --benchmark <tpch|tpcds|ssb> --output <path> [options]\n"
               "Options:\n"
               "  --benchmark, -b <name>   Benchmark to generate\n"
               "  --output, -o <path>      Output path\n"
               "  --scale, --scale-factor <factor>  Scale factor (default: 1)\n"
               "  --dbgen-seed-mode <all-tables|per-table>\n"
               "  --help, -h               Show this help\n";
}

benchgen::SuiteId ResolveSuite(int argc, char** argv, std::string* error) {
  benchgen::SuiteId suite = benchgen::SuiteId::kUnknown;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto parse_value = [&](const std::string& value) -> bool {
      benchgen::SuiteId parsed = benchgen::SuiteIdFromString(value);
      if (parsed == benchgen::SuiteId::kUnknown) {
        if (error) {
          *error = "Unknown benchmark: " + value;
        }
        return false;
      }
      if (suite != benchgen::SuiteId::kUnknown) {
        if (error) {
          *error = "Duplicate --benchmark";
        }
        return false;
      }
      suite = parsed;
      return true;
    };

    if (arg == "--benchmark" || arg == "-b") {
      if (i + 1 >= argc) {
        if (error) {
          *error = "Missing value for --benchmark";
        }
        return benchgen::SuiteId::kUnknown;
      }
      if (!parse_value(argv[++i])) {
        return benchgen::SuiteId::kUnknown;
      }
      continue;
    }

    const std::string kPrefix = "--benchmark=";
    if (arg.rfind(kPrefix, 0) == 0) {
      std::string value = arg.substr(kPrefix.size());
      if (value.empty()) {
        if (error) {
          *error = "Missing value for --benchmark";
        }
        return benchgen::SuiteId::kUnknown;
      }
      if (!parse_value(value)) {
        return benchgen::SuiteId::kUnknown;
      }
      continue;
    }
  }

  return suite;
}

}  // namespace

int main(int argc, char** argv) {
  if (benchgen::cli::HasHelpArg(argc, argv)) {
    PrintUsage(argv[0]);
    return 0;
  }

  std::string error;
  benchgen::SuiteId suite_id = ResolveSuite(argc, argv, &error);
  if (suite_id == benchgen::SuiteId::kUnknown) {
    if (error.empty()) {
      error = "Missing required --benchmark";
    }
    std::cerr << error << "\n";
    PrintUsage(argv[0]);
    return 1;
  }

  benchgen::cli::SchemaArgs args;
  if (!benchgen::cli::ParseSchemaArgs(argc, argv, &args, &error)) {
    std::cerr << error << "\n";
    PrintUsage(argv[0]);
    return 1;
  }

  std::unique_ptr<benchgen::BenchmarkSuite> suite =
      benchgen::MakeBenchmarkSuite(suite_id);
  if (!suite) {
    std::cerr << "Failed to resolve benchmark suite\n";
    return 1;
  }

  std::ofstream out(args.output_path);
  if (!out.is_open()) {
    std::cerr << "Failed to open output file: " << args.output_path << "\n";
    return 1;
  }

  benchgen::GeneratorOptions options;
  options.scale_factor = args.scale_factor;
  options.seed_mode = args.seed_mode;

  if (!benchgen::cli::WriteSchemaJsonForSuite(&out, options, *suite, &error)) {
    std::cerr << error << "\n";
    return 1;
  }

  return 0;
}
