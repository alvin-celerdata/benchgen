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

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <exception>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include "benchgen/arrow_compat.h"
#include "benchgen/benchmark_suite.h"
#include "benchgen/generator_options.h"
#include "benchgen/record_batch_iterator.h"

namespace benchgen::cli {

struct SchemaArgs {
  std::string output_path;
  double scale_factor = 1.0;
  DbgenSeedMode seed_mode = DbgenSeedMode::kPerTable;
};

inline bool IsHelpArg(std::string_view arg) {
  return arg == "--help" || arg == "-h";
}

inline bool HasHelpArg(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (IsHelpArg(argv[i])) {
      return true;
    }
  }
  return false;
}

inline bool ReadDouble(const char* value, double* out) {
  try {
    *out = std::stod(value);
  } catch (const std::exception&) {
    return false;
  }
  return true;
}

inline bool ParseSeedMode(const std::string& value, DbgenSeedMode* out) {
  if (value == "all-tables") {
    *out = DbgenSeedMode::kAllTables;
    return true;
  }
  if (value == "per-table") {
    *out = DbgenSeedMode::kPerTable;
    return true;
  }
  return false;
}

inline bool ParseSchemaArgs(int argc, char** argv, SchemaArgs* args,
                            std::string* error) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto require_value = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        if (error) {
          *error = std::string("Missing value for ") + name;
        }
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
        if (error) {
          *error = "Missing value for --benchmark";
        }
        return false;
      }
      continue;
    }
    if (arg == "--output" || arg == "-o") {
      const char* value = require_value("--output");
      if (!value) return false;
      args->output_path = value;
      continue;
    }
    if (arg == "--scale" || arg == "--scale-factor") {
      const char* value = require_value("--scale");
      if (!value) return false;
      if (!ReadDouble(value, &args->scale_factor)) {
        if (error) {
          *error = "Invalid scale factor";
        }
        return false;
      }
      continue;
    }
    if (arg == "--dbgen-seed-mode") {
      const char* value = require_value("--dbgen-seed-mode");
      if (!value) return false;
      if (!ParseSeedMode(value, &args->seed_mode)) {
        if (error) {
          *error =
              "Invalid --dbgen-seed-mode (expected all-tables or per-table)";
        }
        return false;
      }
      continue;
    }

    if (error) {
      *error = "Unknown argument: " + arg;
    }
    return false;
  }

  if (args->output_path.empty()) {
    if (error) {
      *error = "Missing required --output";
    }
    return false;
  }

  return true;
}

inline std::string EscapeJson(std::string_view input) {
  std::string output;
  output.reserve(input.size());
  for (unsigned char c : input) {
    switch (c) {
      case '"':
        output += "\\\"";
        break;
      case '\\':
        output += "\\\\";
        break;
      case '\b':
        output += "\\b";
        break;
      case '\f':
        output += "\\f";
        break;
      case '\n':
        output += "\\n";
        break;
      case '\r':
        output += "\\r";
        break;
      case '\t':
        output += "\\t";
        break;
      default:
        if (c < 0x20) {
          char buf[7];
          std::snprintf(buf, sizeof(buf), "\\u%04x",
                        static_cast<unsigned int>(c));
          output += buf;
        } else {
          output.push_back(static_cast<char>(c));
        }
        break;
    }
  }
  return output;
}

inline void WriteJsonString(std::ostream* out, std::string_view value) {
  *out << "\"" << EscapeJson(value) << "\"";
}

template <typename TableId, typename TableIdToStringFn, typename MakeIteratorFn>
bool WriteSchemaJsonForTables(std::ostream* out,
                              const GeneratorOptions& options,
                              TableIdToStringFn table_to_string,
                              MakeIteratorFn make_iterator,
                              std::string* error) {
  if (out == nullptr) {
    if (error) {
      *error = "output stream must not be null";
    }
    return false;
  }

  const int table_count = static_cast<int>(TableId::kTableCount);
  *out << "{\n  \"tables\": [\n";
  for (int i = 0; i < table_count; ++i) {
    if (i > 0) {
      *out << ",\n";
    }
    auto table = static_cast<TableId>(i);
    std::string_view table_name = table_to_string(table);
    std::string table_label(table_name);
    std::unique_ptr<RecordBatchIterator> iter;
    try {
      auto status = make_iterator(table, options, &iter);
      if (!status.ok()) {
        if (error) {
          *error = "failed to build schema for table " + table_label + ": " +
                   status.ToString();
        }
        return false;
      }
    } catch (const std::exception& exc) {
      if (error) {
        *error = "failed to build schema for table " + table_label + ": " +
                 exc.what();
      }
      return false;
    } catch (...) {
      if (error) {
        *error = "failed to build schema for table " + table_label +
                 ": unknown error";
      }
      return false;
    }

    std::shared_ptr<arrow::Schema> schema = iter->schema();
    if (!schema) {
      if (error) {
        *error = "schema is null for table " + table_label;
      }
      return false;
    }

    *out << "    {\n";
    *out << "      \"name\": ";
    WriteJsonString(out, table_name);
    *out << ",\n";
    *out << "      \"columns\": [\n";
    for (int col = 0; col < schema->num_fields(); ++col) {
      if (col > 0) {
        *out << ",\n";
      }
      const auto& field = schema->field(col);
      *out << "        {\"name\": ";
      WriteJsonString(out, field->name());
      *out << ", \"type\": ";
      WriteJsonString(out, field->type()->ToString());
      *out << "}";
    }
    *out << "\n      ]\n";
    *out << "    }";
  }
  *out << "\n  ]\n}\n";

  return true;
}

inline bool WriteSchemaJsonForSuite(std::ostream* out,
                                    const GeneratorOptions& options,
                                    const BenchmarkSuite& suite,
                                    std::string* error) {
  if (out == nullptr) {
    if (error) {
      *error = "output stream must not be null";
    }
    return false;
  }

  const int table_count = suite.table_count();
  *out << "{\n  \"tables\": [\n";
  for (int i = 0; i < table_count; ++i) {
    if (i > 0) {
      *out << ",\n";
    }
    std::string_view table_name = suite.TableName(i);
    if (table_name.empty()) {
      if (error) {
        *error = "failed to resolve table name at index " + std::to_string(i);
      }
      return false;
    }
    std::string table_label(table_name);
    std::unique_ptr<RecordBatchIterator> iter;
    try {
      auto status = suite.MakeIterator(table_name, options, &iter);
      if (!status.ok()) {
        if (error) {
          *error = "failed to build schema for table " + table_label + ": " +
                   status.ToString();
        }
        return false;
      }
    } catch (const std::exception& exc) {
      if (error) {
        *error = "failed to build schema for table " + table_label + ": " +
                 exc.what();
      }
      return false;
    } catch (...) {
      if (error) {
        *error = "failed to build schema for table " + table_label +
                 ": unknown error";
      }
      return false;
    }

    std::shared_ptr<arrow::Schema> schema = iter->schema();
    if (!schema) {
      if (error) {
        *error = "schema is null for table " + table_label;
      }
      return false;
    }

    *out << "    {\n";
    *out << "      \"name\": ";
    WriteJsonString(out, table_name);
    *out << ",\n";
    *out << "      \"columns\": [\n";
    for (int col = 0; col < schema->num_fields(); ++col) {
      if (col > 0) {
        *out << ",\n";
      }
      const auto& field = schema->field(col);
      *out << "        {\"name\": ";
      WriteJsonString(out, field->name());
      *out << ", \"type\": ";
      WriteJsonString(out, field->type()->ToString());
      *out << "}";
    }
    *out << "\n      ]\n";
    *out << "    }";
  }
  *out << "\n  ]\n}\n";

  return true;
}

}  // namespace benchgen::cli
