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
#include <atomic>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "benchgen/arrow_compat.h"
#include "benchgen/benchmark_suite.h"
#include "benchgen/generator_options.h"
#include "benchgen/record_batch_iterator_factory.h"
#include "common/gen_table_args.h"
#include "util/record_batch_writer.h"

namespace {

bool IsHelpArg(const std::string& arg) {
  return arg == "--help" || arg == "-h";
}

bool HasHelpArg(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (IsHelpArg(argv[i])) {
      return true;
    }
  }
  return false;
}

bool ReadInt64(const char* value, int64_t* out) {
  try {
    *out = std::stoll(value);
  } catch (const std::exception&) {
    return false;
  }
  return true;
}

bool ReadDouble(const char* value, double* out) {
  try {
    *out = std::stod(value);
  } catch (const std::exception&) {
    return false;
  }
  return true;
}

struct ParallelRange {
  int64_t start_row = 0;
  int64_t row_count = 0;
};

ParallelRange SplitRange(int64_t total_rows, int64_t parallel_count,
                         int64_t parallel_index) {
  ParallelRange range;
  if (total_rows <= 0 || parallel_count <= 0 || parallel_index < 0) {
    return range;
  }
  int64_t base_rows = total_rows / parallel_count;
  int64_t remainder = total_rows % parallel_count;
  range.start_row =
      base_rows * parallel_index + std::min(parallel_index, remainder);
  range.row_count = base_rows + (parallel_index < remainder ? 1 : 0);
  return range;
}

bool ResolveTableRowCount(const benchgen::BenchmarkSuite& suite,
                          const benchgen::cli::GenTableArgs& args,
                          int64_t* out, bool* known, std::string* error) {
  if (out == nullptr || known == nullptr) {
    if (error) {
      *error = "Row count output parameters must not be null";
    }
    return false;
  }
  benchgen::GeneratorOptions options;
  options.scale_factor = args.scale_factor;
  options.seed_mode = args.seed_mode;

  auto status = suite.ResolveTableRowCount(args.table, options, out, known);
  if (!status.ok()) {
    if (error) {
      *error = status.ToString();
    }
    return false;
  }
  return true;
}

bool ResolveParallelRanges(const benchgen::BenchmarkSuite& suite,
                           const benchgen::cli::GenTableArgs& args,
                           std::vector<ParallelRange>* ranges,
                           std::string* error) {
  if (ranges == nullptr) {
    if (error) {
      *error = "Parallel range output must not be null";
    }
    return false;
  }
  ranges->clear();
  if (args.parallel <= 1) {
    return true;
  }
  if (args.parallel <= 0) {
    if (error) {
      *error = "Parallel count must be positive";
    }
    return false;
  }
  if (args.table.empty()) {
    if (error) {
      *error = "--table is required for parallel generation";
    }
    return false;
  }
  if (args.start_row < 0) {
    if (error) {
      *error = "Start row must be non-negative";
    }
    return false;
  }

  int64_t total_rows = 0;
  bool known = false;
  if (!ResolveTableRowCount(suite, args, &total_rows, &known, error)) {
    return false;
  }
  if (!known) {
    return true;
  }

  int64_t available = total_rows - args.start_row;
  if (available < 0) {
    available = 0;
  }
  int64_t base_count = args.row_count < 0
                           ? available
                           : std::min(args.row_count, available);
  if (base_count <= 0) {
    return true;
  }

  int64_t parallel = std::min(args.parallel, base_count);
  if (parallel <= 1) {
    return true;
  }

  ranges->reserve(static_cast<size_t>(parallel));
  for (int64_t i = 0; i < parallel; ++i) {
    ParallelRange range = SplitRange(base_count, parallel, i);
    range.start_row += args.start_row;
    ranges->push_back(range);
  }
  return true;
}

std::string BuildParallelOutputPath(const std::string& output, int64_t index) {
  std::filesystem::path path(output);
  std::string suffix = "-" + std::to_string(index);
  if (path.has_extension()) {
    std::string stem = path.stem().string();
    std::string extension = path.extension().string();
    return (path.parent_path() / (stem + suffix + extension)).string();
  }
  return output + suffix;
}

void PrintUsage(const char* argv0) {
  std::cerr
      << "Usage: " << argv0
      << " --benchmark <tpch|tpcds|ssb> --table <name> [options]\n"
         "Common options:\n"
         "  --benchmark, -b <name>   Benchmark to generate\n"
         "  --table, -t <name>       Table name\n"
         "  --scale, --scale-factor, -s <factor>  Scale factor (default: 1)\n"
         "  --chunk-size <rows>      Rows per RecordBatch (default: 10000)\n"
         "  --start-row <row>        0-based row offset (default: 0)\n"
         "  --row-count <rows>       Rows to generate (default: -1 = to end)\n"
         "  --output, -o <path>      Output path (default: stdout)\n"
         "                           TPC-DS requires --output\n"
         "  --dbgen-seed-mode <all-tables|per-table>  Seed init (default: per-table)\n"
         "  --help, -h               Show this help\n"
         "Parallel options:\n"
         "  --parallel, -p <count>\n"
         "                           Worker threads (default: 1)\n"
         "                           Uses <output>-<index> (before extension)\n"
         "                           Runs serially when total rows are unknown\n";
}

benchgen::SuiteId ResolveBenchmark(int argc, char** argv, std::string* error) {
  benchgen::SuiteId benchmark = benchgen::SuiteId::kUnknown;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--benchmark" || arg == "-b") {
      if (i + 1 >= argc) {
        *error = "Missing value for --benchmark";
        return benchgen::SuiteId::kUnknown;
      }
      std::string value = argv[++i];
      benchgen::SuiteId parsed = benchgen::SuiteIdFromString(value);
      if (parsed == benchgen::SuiteId::kUnknown) {
        *error = "Unknown benchmark: " + value;
        return benchgen::SuiteId::kUnknown;
      }
      if (benchmark != benchgen::SuiteId::kUnknown) {
        *error = "Duplicate --benchmark";
        return benchgen::SuiteId::kUnknown;
      }
      benchmark = parsed;
      continue;
    }

    const std::string kPrefix = "--benchmark=";
    if (arg.rfind(kPrefix, 0) == 0) {
      std::string value = arg.substr(kPrefix.size());
      if (value.empty()) {
        *error = "Missing value for --benchmark";
        return benchgen::SuiteId::kUnknown;
      }
      benchgen::SuiteId parsed = benchgen::SuiteIdFromString(value);
      if (parsed == benchgen::SuiteId::kUnknown) {
        *error = "Unknown benchmark: " + value;
        return benchgen::SuiteId::kUnknown;
      }
      if (benchmark != benchgen::SuiteId::kUnknown) {
        *error = "Duplicate --benchmark";
        return benchgen::SuiteId::kUnknown;
      }
      benchmark = parsed;
      continue;
    }
  }

  return benchmark;
}

bool ParseArgs(int argc, char** argv, benchgen::SuiteId benchmark,
               benchgen::cli::GenTableArgs* args, std::string* error) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto require_value = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        *error = std::string("Missing value for ") + name;
        return nullptr;
      }
      return argv[++i];
    };

    if (IsHelpArg(arg)) {
      continue;
    }
    if (arg == "--benchmark" || arg == "-b") {
      if (!require_value("--benchmark")) {
        return false;
      }
      continue;
    }
    const std::string kBenchmarkPrefix = "--benchmark=";
    if (arg.rfind(kBenchmarkPrefix, 0) == 0) {
      if (arg.size() == kBenchmarkPrefix.size()) {
        *error = "Missing value for --benchmark";
        return false;
      }
      continue;
    }
    if (arg == "--table" || arg == "-t") {
      const char* value = require_value("--table");
      if (!value) return false;
      args->table = value;
      continue;
    }
    if (arg == "--scale" || arg == "--scale-factor" || arg == "-s") {
      const char* value = require_value("--scale");
      if (!value) return false;
      if (!ReadDouble(value, &args->scale_factor)) {
        *error = "Invalid scale factor";
        return false;
      }
      continue;
    }
    if (arg == "--chunk-size") {
      const char* value = require_value("--chunk-size");
      if (!value) return false;
      if (!ReadInt64(value, &args->chunk_size)) {
        *error = "Invalid chunk size";
        return false;
      }
      continue;
    }
    if (arg == "--start-row") {
      const char* value = require_value("--start-row");
      if (!value) return false;
      if (!ReadInt64(value, &args->start_row)) {
        *error = "Invalid start row";
        return false;
      }
      continue;
    }
    if (arg == "--row-count") {
      const char* value = require_value("--row-count");
      if (!value) return false;
      if (!ReadInt64(value, &args->row_count)) {
        *error = "Invalid row count";
        return false;
      }
      continue;
    }
    if (arg == "--output" || arg == "-o") {
      const char* value = require_value("--output");
      if (!value) return false;
      args->output = value;
      continue;
    }
    if (arg == "--parallel" || arg == "-p") {
      const char* value = require_value("--parallel");
      if (!value) return false;
      if (!ReadInt64(value, &args->parallel)) {
        *error = "Invalid parallel value";
        return false;
      }
      continue;
    }
    if (arg == "--dbgen-seed-mode") {
      const char* value = require_value("--dbgen-seed-mode");
      if (!value) return false;
      std::string mode = value;
      if (mode == "all-tables") {
        args->seed_mode = benchgen::DbgenSeedMode::kAllTables;
      } else if (mode == "per-table") {
        args->seed_mode = benchgen::DbgenSeedMode::kPerTable;
      } else {
        *error = "Unknown dbgen seed mode: " + mode;
        return false;
      }
      continue;
    }

    *error = "Unknown argument: " + arg;
    return false;
  }

  return true;
}

struct SuiteConfig {
  benchgen::internal::RecordBatchWriterFormat writer_format;
  bool require_output = false;
  std::ios::openmode output_mode = std::ios::out | std::ios::binary;
};

bool ResolveSuiteConfig(const benchgen::BenchmarkSuite& suite,
                        SuiteConfig* config, std::string* error) {
  if (config == nullptr) {
    if (error) {
      *error = "Suite config must not be null";
    }
    return false;
  }

  switch (suite.suite_id()) {
    case benchgen::SuiteId::kTpch:
      config->writer_format =
          benchgen::internal::RecordBatchWriterFormat::kTpch;
      config->require_output = false;
      config->output_mode = std::ios::out | std::ios::binary;
      return true;
    case benchgen::SuiteId::kTpcds:
      config->writer_format =
          benchgen::internal::RecordBatchWriterFormat::kTpcds;
      config->require_output = true;
      config->output_mode = std::ios::out | std::ios::binary;
      return true;
    case benchgen::SuiteId::kSsb:
      config->writer_format = benchgen::internal::RecordBatchWriterFormat::kSsb;
      config->require_output = false;
      config->output_mode = std::ios::out;
      return true;
    case benchgen::SuiteId::kUnknown:
      break;
  }

  if (error) {
    *error = "Unknown benchmark suite";
  }
  return false;
}

bool ValidateSuiteArgs(const benchgen::BenchmarkSuite& suite,
                       const benchgen::cli::GenTableArgs& args,
                       std::string* error) {
  (void)suite;
  if (args.start_row < 0) {
    if (error) {
      *error = "Start row must be non-negative";
    }
    return false;
  }
  if (args.parallel <= 0) {
    if (error) {
      *error = "Parallel count must be positive";
    }
    return false;
  }

  return true;
}

int RunSuiteGenTable(const benchgen::BenchmarkSuite& suite,
                     const benchgen::cli::GenTableArgs& args,
                     const SuiteConfig& config) {
  if (args.table.empty()) {
    std::cerr << "--table is required\n";
    return 1;
  }
  if (config.require_output && args.output.empty()) {
    std::cerr << "Output path is required\n";
    return 1;
  }

  benchgen::GeneratorOptions options;
  options.scale_factor = args.scale_factor;
  options.chunk_size = args.chunk_size;
  options.start_row = args.start_row;
  options.row_count = args.row_count;
  options.seed_mode = args.seed_mode;

  std::unique_ptr<benchgen::RecordBatchIterator> iterator;
  auto status = suite.MakeIterator(args.table, options, &iterator);
  if (!status.ok()) {
    std::cerr << "Failed to create generator: " << status.ToString() << "\n";
    return 1;
  }

  std::ofstream file;
  std::ostream* output = &std::cout;
  if (!args.output.empty()) {
    file.open(args.output, config.output_mode);
    if (!file) {
      std::cerr << "Failed to open output file: " << args.output << "\n";
      return 1;
    }
    output = &file;
  }

  benchgen::internal::RecordBatchWriter writer(config.writer_format);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    status = iterator->Next(&batch);
    if (!status.ok()) {
      std::cerr << "Error generating batch: " << status.ToString() << "\n";
      return 1;
    }
    if (!batch) {
      break;
    }
    status = writer.Write(output, batch);
    if (!status.ok()) {
      std::cerr << "Error writing batch: " << status.ToString() << "\n";
      return 1;
    }
  }

  return 0;
}

int RunSuiteGenTableParallel(const benchgen::BenchmarkSuite& suite,
                             const benchgen::cli::GenTableArgs& args,
                             const SuiteConfig& config,
                             const std::vector<ParallelRange>& ranges) {
  if (args.output.empty()) {
    std::cerr << "Output path is required for parallel generation\n";
    return 1;
  }

  std::vector<std::string> part_paths;
  part_paths.reserve(ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    part_paths.push_back(
        BuildParallelOutputPath(args.output, static_cast<int64_t>(i)));
  }

  std::atomic<bool> failed(false);
  std::mutex mutex;
  int status = 0;
  std::string status_message;

  auto worker = [&](size_t index) {
    if (failed.load()) {
      return;
    }
    benchgen::cli::GenTableArgs part_args = args;
    part_args.start_row = ranges[index].start_row;
    part_args.row_count = ranges[index].row_count;
    part_args.output = part_paths[index];
    part_args.parallel = 1;

    int result = RunSuiteGenTable(suite, part_args, config);
    if (result != 0) {
      failed.store(true);
      std::lock_guard<std::mutex> lock(mutex);
      if (status == 0) {
        status = result;
        status_message =
            "Parallel worker failed for part " + std::to_string(index);
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    threads.emplace_back(worker, i);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  if (status != 0) {
    if (!status_message.empty()) {
      std::cerr << status_message << "\n";
    }
    return status;
  }

  return 0;
}

int RunSuiteWithConfig(const benchgen::BenchmarkSuite& suite,
                       const benchgen::cli::GenTableArgs& args) {
  SuiteConfig config;
  std::string error;
  if (!ResolveSuiteConfig(suite, &config, &error)) {
    std::cerr << error << "\n";
    return 1;
  }
  if (config.require_output && args.output.empty()) {
    std::cerr << "Output path is required\n";
    return 1;
  }
  if (!ValidateSuiteArgs(suite, args, &error)) {
    std::cerr << error << "\n";
    return 1;
  }
  std::vector<ParallelRange> ranges;
  if (!ResolveParallelRanges(suite, args, &ranges, &error)) {
    std::cerr << error << "\n";
    return 1;
  }
  if (!ranges.empty()) {
    return RunSuiteGenTableParallel(suite, args, config, ranges);
  }
  return RunSuiteGenTable(suite, args, config);
}

}  // namespace

int main(int argc, char** argv) {
  bool show_help = HasHelpArg(argc, argv);
  std::string error;
  benchgen::SuiteId benchmark = ResolveBenchmark(argc, argv, &error);
  if (!error.empty() && !show_help) {
    std::cerr << error << "\n";
    PrintUsage(argv[0]);
    return 1;
  }
  if (show_help) {
    PrintUsage(argv[0]);
    return 0;
  }

  benchgen::cli::GenTableArgs args;
  if (!ParseArgs(argc, argv, benchmark, &args, &error)) {
    std::cerr << error << "\n";
    PrintUsage(argv[0]);
    return 1;
  }

  if (benchmark == benchgen::SuiteId::kUnknown) {
    std::cerr << "--benchmark is required\n";
    PrintUsage(argv[0]);
    return 1;
  }

  auto suite = benchgen::MakeBenchmarkSuite(benchmark);
  if (!suite) {
    std::cerr << "Unknown benchmark\n";
    PrintUsage(argv[0]);
    return 1;
  }

  return RunSuiteWithConfig(*suite, args);
}
