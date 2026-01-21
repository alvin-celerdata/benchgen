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

#include "distribution/dst_distribution_store.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "distribution/embedded_distribution.h"

namespace benchgen::tpcds::internal {
namespace {

std::string Trim(std::string_view input) {
  size_t start = 0;
  while (start < input.size() &&
         std::isspace(static_cast<unsigned char>(input[start]))) {
    ++start;
  }
  size_t end = input.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return std::string(input.substr(start, end - start));
}

std::string ToLower(std::string_view input) {
  std::string output;
  output.reserve(input.size());
  for (char c : input) {
    output.push_back(
        static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return output;
}

bool StartsWithIgnoreCase(std::string_view input, std::string_view prefix) {
  if (input.size() < prefix.size()) {
    return false;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(input[i])) !=
        std::tolower(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
  }
  return true;
}

std::string Unescape(std::string_view input) {
  std::string output;
  output.reserve(input.size());
  bool escape = false;
  for (char c : input) {
    if (escape) {
      output.push_back(c);
      escape = false;
      continue;
    }
    if (c == '\\') {
      escape = true;
      continue;
    }
    output.push_back(c);
  }
  return output;
}

bool IsValidUtf8(std::string_view input) {
  int remaining = 0;
  for (unsigned char c : input) {
    if (remaining == 0) {
      if (c <= 0x7F) {
        continue;
      }
      if (c >= 0xC2 && c <= 0xDF) {
        remaining = 1;
      } else if (c >= 0xE0 && c <= 0xEF) {
        remaining = 2;
      } else if (c >= 0xF0 && c <= 0xF4) {
        remaining = 3;
      } else {
        return false;
      }
    } else {
      if (c < 0x80 || c > 0xBF) {
        return false;
      }
      --remaining;
    }
  }
  return remaining == 0;
}

std::string Latin1ToUtf8(std::string_view input) {
  std::string output;
  output.reserve(input.size());
  for (unsigned char c : input) {
    if (c < 0x80) {
      output.push_back(static_cast<char>(c));
    } else {
      output.push_back(static_cast<char>(0xC0 | (c >> 6)));
      output.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    }
  }
  return output;
}

std::string NormalizeValueEncoding(std::string_view input) {
  std::string unescaped = Unescape(input);
  if (IsValidUtf8(unescaped)) {
    return unescaped;
  }
  return Latin1ToUtf8(unescaped);
}

std::string DirectoryFromPath(const std::string& path) {
  size_t slash = path.find_last_of("/\\");
  if (slash == std::string::npos) {
    return std::string();
  }
  return path.substr(0, slash);
}

std::string TrimInclude(std::string_view input) {
  std::string trimmed = Trim(input);
  if (trimmed.empty()) {
    return std::string();
  }
  size_t start = trimmed.find_first_not_of(" \t");
  if (start == std::string::npos) {
    return std::string();
  }
  char quote = trimmed[start];
  if (quote == '"' || quote == '<') {
    char end_quote = quote == '"' ? '"' : '>';
    size_t end = trimmed.find(end_quote, start + 1);
    if (end != std::string::npos && end > start + 1) {
      return std::string(trimmed.substr(start + 1, end - start - 1));
    }
  }
  size_t end = trimmed.find_first_of(" \t;");
  if (end == std::string::npos) {
    end = trimmed.size();
  }
  return std::string(trimmed.substr(start, end - start));
}

constexpr int kIdxNameLength = 20;
constexpr int kIdxIntCount = 7;
constexpr int kIdxEntrySize = kIdxNameLength + kIdxIntCount * 4;

constexpr int kTknVarchar = 6;
constexpr int kTknInt = 7;
constexpr int kTknDate = 9;
constexpr int kTknDecimal = 10;

int32_t ReadBE32(std::istream* file) {
  unsigned char buffer[4] = {0, 0, 0, 0};
  file->read(reinterpret_cast<char*>(buffer), sizeof(buffer));
  if (!file->good()) {
    throw std::runtime_error("unexpected end of idx file");
  }
  return static_cast<int32_t>((static_cast<uint32_t>(buffer[0]) << 24) |
                              (static_cast<uint32_t>(buffer[1]) << 16) |
                              (static_cast<uint32_t>(buffer[2]) << 8) |
                              static_cast<uint32_t>(buffer[3]));
}

DstValueType MapIdxType(int32_t token) {
  switch (token) {
    case kTknVarchar:
      return DstValueType::kVarchar;
    case kTknInt:
      return DstValueType::kInt;
    case kTknDate:
      return DstValueType::kDate;
    case kTknDecimal:
      return DstValueType::kDecimal;
    default:
      throw std::runtime_error("unknown idx distribution type: " +
                               std::to_string(token));
  }
}

struct IdxEntry {
  std::string name;
  int32_t offset = 0;
  int32_t str_space = 0;
  int32_t length = 0;
  int32_t w_width = 0;
  int32_t v_width = 0;
  int32_t name_space = 0;
};

size_t SafeStrnLen(const char* input, size_t max_len) {
  size_t len = 0;
  while (len < max_len && input[len] != '\0') {
    ++len;
  }
  return len;
}

size_t FindUnquoted(std::string_view input, char target) {
  bool in_quotes = false;
  bool escape = false;
  for (size_t i = 0; i < input.size(); ++i) {
    char c = input[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (c == '\\' && in_quotes) {
      escape = true;
      continue;
    }
    if (c == '"') {
      in_quotes = !in_quotes;
      continue;
    }
    if (!in_quotes && c == target) {
      return i;
    }
  }
  return std::string::npos;
}

std::vector<std::string> SplitFields(std::string_view input,
                                     bool split_on_colon) {
  std::string trimmed = Trim(input);
  std::vector<std::string> fields;
  std::string current;
  bool in_quotes = false;
  bool escape = false;
  bool field_was_quoted = false;
  auto flush_field = [&](bool force_empty) {
    if (!current.empty() || force_empty || field_was_quoted) {
      if (field_was_quoted) {
        fields.push_back(current);
      } else {
        fields.push_back(Trim(current));
      }
    }
    current.clear();
    field_was_quoted = false;
  };
  for (char c : trimmed) {
    if (escape) {
      current.push_back(c);
      escape = false;
      continue;
    }
    if (c == '\\' && in_quotes) {
      escape = true;
      continue;
    }
    if (c == '"') {
      in_quotes = !in_quotes;
      field_was_quoted = true;
      continue;
    }
    if (!in_quotes &&
        (c == ',' || std::isspace(static_cast<unsigned char>(c)) ||
         (split_on_colon && c == ':'))) {
      bool force_empty = (c == ',' || (split_on_colon && c == ':'));
      flush_field(force_empty);
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty() || !fields.empty() || !trimmed.empty() ||
      field_was_quoted) {
    flush_field(false);
  }

  for (auto& field : fields) {
    field = NormalizeValueEncoding(field);
  }
  return fields;
}

std::vector<std::string> SplitFields(std::string_view input) {
  return SplitFields(input, false);
}

DstValueType ParseValueType(std::string_view token) {
  std::string value = ToLower(Trim(token));
  if (value == "varchar" || value == "char") {
    return DstValueType::kVarchar;
  }
  if (value == "int" || value == "integer") {
    return DstValueType::kInt;
  }
  if (value == "date") {
    return DstValueType::kDate;
  }
  if (value == "decimal") {
    return DstValueType::kDecimal;
  }
  throw std::runtime_error("unknown distribution type: " + std::string(token));
}

std::vector<DstValueType> ParseTypes(std::string_view input) {
  std::vector<DstValueType> types;
  for (const auto& token : SplitFields(input)) {
    if (!token.empty()) {
      types.push_back(ParseValueType(token));
    }
  }
  return types;
}

int ParseWeightToken(std::string_view token) {
  if (token.empty()) {
    return 0;
  }
  std::string token_str(token);
  char* end = nullptr;
  long value = std::strtol(token_str.c_str(), &end, 10);
  if (end == token_str.c_str()) {
    return 0;
  }
  return static_cast<int>(value);
}

void ParseNames(std::string_view input, std::vector<std::string>* value_names,
                std::vector<std::string>* weight_names) {
  size_t colon = FindUnquoted(input, ':');
  std::string_view values_part =
      colon == std::string::npos ? input : input.substr(0, colon);
  std::string_view weights_part =
      colon == std::string::npos ? std::string_view{} : input.substr(colon + 1);
  *value_names = SplitFields(values_part);
  if (!weights_part.empty()) {
    *weight_names = SplitFields(weights_part);
  }
}

struct ParsedAdd {
  std::vector<std::string> values;
  std::vector<int> weights;
};

ParsedAdd ParseAddEntry(std::string_view input, int expected_values,
                        int expected_weights) {
  ParsedAdd entry;
  std::vector<std::string> tokens = SplitFields(input, true);
  if (expected_values > 0) {
    if (static_cast<int>(tokens.size()) < expected_values) {
      return entry;
    }
    entry.values.assign(tokens.begin(), tokens.begin() + expected_values);
  }
  if (expected_weights > 0) {
    if (static_cast<int>(tokens.size()) < expected_values + expected_weights) {
      return entry;
    }
    entry.weights.reserve(static_cast<size_t>(expected_weights));
    for (int i = 0; i < expected_weights; ++i) {
      const auto& token = tokens[static_cast<size_t>(expected_values + i)];
      entry.weights.push_back(ParseWeightToken(token));
    }
  }
  return entry;
}

}  // namespace

DstDistributionStore::DstDistributionStore() {
  const EmbeddedDistributionFile* idx_file =
      FindEmbeddedDistributionFile("tpcds.idx");
  if (!idx_file ||
      !LoadIdxData(idx_file->data, idx_file->size, "embedded:tpcds.idx")) {
    throw std::runtime_error("embedded distribution missing tpcds.idx");
  }
}

const DstDistribution& DstDistributionStore::Get(std::string_view name) const {
  std::string key = ToLower(name);
  auto it = distributions_.find(key);
  if (it == distributions_.end()) {
    throw std::runtime_error("missing distribution: " + std::string(name));
  }
  return it->second;
}

bool DstDistributionStore::LoadIdxFile(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    return false;
  }
  return LoadIdxStream(&file, path);
}

bool DstDistributionStore::LoadIdxData(const unsigned char* data, size_t size,
                                       const std::string& label) {
  if (data == nullptr || size == 0) {
    return false;
  }
  std::string buffer(reinterpret_cast<const char*>(data), size);
  std::istringstream stream(buffer, std::ios::binary);
  return LoadIdxStream(&stream, label);
}

bool DstDistributionStore::LoadIdxStream(std::istream* file,
                                         const std::string& path) {
  int32_t entry_count = ReadBE32(file);
  if (entry_count <= 0) {
    throw std::runtime_error("invalid idx entry count in " + path);
  }

  file->seekg(0, std::ios::end);
  std::streamoff file_size = file->tellg();
  if (file_size < static_cast<std::streamoff>(kIdxEntrySize * entry_count)) {
    throw std::runtime_error("invalid idx file size for " + path);
  }
  std::streamoff index_start =
      file_size - static_cast<std::streamoff>(kIdxEntrySize * entry_count);
  file->seekg(index_start, std::ios::beg);

  std::vector<IdxEntry> entries;
  entries.reserve(static_cast<size_t>(entry_count));
  for (int32_t i = 0; i < entry_count; ++i) {
    char name_buffer[kIdxNameLength] = {};
    file->read(name_buffer, kIdxNameLength);
    if (!file->good()) {
      throw std::runtime_error("failed to read idx name in " + path);
    }
    std::string name(name_buffer, static_cast<size_t>(SafeStrnLen(
                                      name_buffer, kIdxNameLength)));
    int32_t index = ReadBE32(file);
    int32_t offset = ReadBE32(file);
    int32_t str_space = ReadBE32(file);
    int32_t length = ReadBE32(file);
    int32_t w_width = ReadBE32(file);
    int32_t v_width = ReadBE32(file);
    int32_t name_space = ReadBE32(file);

    (void)index;
    if (offset < 0 || str_space < 0 || length < 0 || w_width < 0 ||
        v_width < 0 || name_space < 0) {
      throw std::runtime_error("invalid idx entry values in " + path);
    }

    IdxEntry entry;
    entry.name = std::move(name);
    entry.offset = offset;
    entry.str_space = str_space;
    entry.length = length;
    entry.w_width = w_width;
    entry.v_width = v_width;
    entry.name_space = name_space;
    entries.push_back(std::move(entry));
  }

  for (const auto& entry : entries) {
    if (entry.name.empty()) {
      continue;
    }
    file->seekg(entry.offset, std::ios::beg);
    if (!file->good()) {
      throw std::runtime_error("failed to seek idx entry in " + path);
    }

    std::vector<DstValueType> types;
    types.reserve(static_cast<size_t>(entry.v_width));
    for (int32_t i = 0; i < entry.v_width; ++i) {
      types.push_back(MapIdxType(ReadBE32(file)));
    }

    std::vector<std::vector<int32_t>> weights;
    weights.reserve(static_cast<size_t>(entry.w_width));
    for (int32_t i = 0; i < entry.w_width; ++i) {
      std::vector<int32_t> set;
      set.reserve(static_cast<size_t>(entry.length));
      for (int32_t j = 0; j < entry.length; ++j) {
        set.push_back(ReadBE32(file));
      }
      weights.push_back(std::move(set));
    }

    std::vector<std::vector<int32_t>> offsets;
    offsets.reserve(static_cast<size_t>(entry.v_width));
    for (int32_t i = 0; i < entry.v_width; ++i) {
      std::vector<int32_t> set;
      set.reserve(static_cast<size_t>(entry.length));
      for (int32_t j = 0; j < entry.length; ++j) {
        set.push_back(ReadBE32(file));
      }
      offsets.push_back(std::move(set));
    }

    std::vector<char> names_buffer;
    if (entry.name_space > 0) {
      names_buffer.resize(static_cast<size_t>(entry.name_space));
      file->read(names_buffer.data(), entry.name_space);
      if (!file->good()) {
        throw std::runtime_error("failed to read idx names for " + entry.name);
      }
    }

    std::string strings;
    if (entry.str_space > 0) {
      strings.resize(static_cast<size_t>(entry.str_space));
      file->read(strings.data(), entry.str_space);
      if (!file->good()) {
        throw std::runtime_error("failed to read idx strings for " +
                                 entry.name);
      }
    }

    std::vector<std::string> value_names;
    std::vector<std::string> weight_names;
    if (!names_buffer.empty()) {
      std::string current;
      current.reserve(32);
      std::vector<std::string> all_names;
      for (char c : names_buffer) {
        if (c == '\0') {
          all_names.push_back(current);
          current.clear();
        } else {
          current.push_back(c);
        }
      }
      if (!current.empty()) {
        all_names.push_back(current);
      }
      if (static_cast<int32_t>(all_names.size()) >= entry.v_width) {
        value_names.assign(all_names.begin(),
                           all_names.begin() + entry.v_width);
        if (static_cast<int32_t>(all_names.size()) > entry.v_width) {
          weight_names.assign(all_names.begin() + entry.v_width,
                              all_names.end());
        }
      }
    }

    auto read_string = [&](int32_t offset) -> std::string {
      if (offset < 0 || offset >= entry.str_space) {
        throw std::runtime_error("idx string offset out of range for " +
                                 entry.name);
      }
      size_t start = static_cast<size_t>(offset);
      size_t end = strings.find('\0', start);
      if (end == std::string::npos) {
        end = strings.size();
      }
      return strings.substr(start, end - start);
    };

    DstDistribution dist(entry.name);
    dist.SetTypes(std::move(types));
    dist.SetWeightSetCount(entry.w_width);
    if (!value_names.empty() || !weight_names.empty()) {
      dist.SetNames(std::move(value_names), std::move(weight_names));
    }

    for (int32_t row = 0; row < entry.length; ++row) {
      std::vector<std::string> values;
      values.reserve(static_cast<size_t>(entry.v_width));
      for (int32_t v = 0; v < entry.v_width; ++v) {
        values.push_back(read_string(offsets[v][row]));
      }

      std::vector<int> row_weights;
      row_weights.reserve(static_cast<size_t>(entry.w_width));
      for (int32_t w = 0; w < entry.w_width; ++w) {
        row_weights.push_back(static_cast<int>(weights[w][row]));
      }
      dist.AddEntry(values, row_weights);
    }

    AddDistribution(std::move(dist));
  }

  return true;
}

void DstDistributionStore::LoadFile(const std::string& path) {
  if (!loaded_files_.insert(path).second) {
    return;
  }

  std::ifstream file(path);
  if (!file.is_open()) {
    throw std::runtime_error("unable to open distribution file: " + path);
  }

  std::string directory = DirectoryFromPath(path);
  std::string line;
  DstDistribution current;
  bool has_current = false;
  while (std::getline(file, line)) {
    std::string trimmed = Trim(line);
    if (trimmed.empty()) {
      continue;
    }
    if (trimmed.rfind("--", 0) == 0 || trimmed.rfind("-", 0) == 0) {
      continue;
    }

    if (StartsWithIgnoreCase(trimmed, "#include")) {
      std::string include_path = TrimInclude(trimmed.substr(8));
      if (!include_path.empty()) {
        if (!directory.empty() && include_path.front() != '/' &&
            include_path.front() != '\\') {
          include_path = directory + "/" + include_path;
        }
        LoadFile(include_path);
      }
      continue;
    }

    if (StartsWithIgnoreCase(trimmed, "create ")) {
      if (has_current) {
        AddDistribution(std::move(current));
        current = DstDistribution();
      }
      std::string name = Trim(trimmed.substr(6));
      if (!name.empty() && name.back() == ';') {
        name.pop_back();
      }
      current = DstDistribution(Trim(name));
      has_current = true;
      continue;
    }

    if (!has_current) {
      continue;
    }

    if (StartsWithIgnoreCase(trimmed, "set types")) {
      size_t open = trimmed.find('(');
      size_t close = trimmed.rfind(')');
      if (open == std::string::npos || close == std::string::npos ||
          close <= open) {
        throw std::runtime_error("invalid types line in " + path);
      }
      current.SetTypes(ParseTypes(trimmed.substr(open + 1, close - open - 1)));
      continue;
    }

    if (StartsWithIgnoreCase(trimmed, "set weights")) {
      size_t eq = trimmed.find('=');
      if (eq == std::string::npos) {
        throw std::runtime_error("invalid weights line in " + path);
      }
      std::string count = Trim(trimmed.substr(eq + 1));
      if (!count.empty() && count.back() == ';') {
        count.pop_back();
      }
      current.SetWeightSetCount(std::stoi(count));
      continue;
    }

    if (StartsWithIgnoreCase(trimmed, "set names")) {
      size_t open = trimmed.find('(');
      size_t close = trimmed.rfind(')');
      if (open == std::string::npos || close == std::string::npos ||
          close <= open) {
        throw std::runtime_error("invalid names line in " + path);
      }
      std::vector<std::string> value_names;
      std::vector<std::string> weight_names;
      ParseNames(trimmed.substr(open + 1, close - open - 1), &value_names,
                 &weight_names);
      current.SetNames(std::move(value_names), std::move(weight_names));
      continue;
    }

    if (StartsWithIgnoreCase(trimmed, "add")) {
      size_t open = trimmed.find('(');
      size_t close = trimmed.rfind(')');
      if (open == std::string::npos || close == std::string::npos ||
          close <= open) {
        throw std::runtime_error("invalid add line in " + path);
      }
      ParsedAdd entry =
          ParseAddEntry(trimmed.substr(open + 1, close - open - 1),
                        current.value_set_count(), current.weight_set_count());
      current.AddEntry(entry.values, entry.weights);
      continue;
    }
  }

  if (has_current) {
    AddDistribution(std::move(current));
  }
}

void DstDistributionStore::AddDistribution(DstDistribution distribution) {
  std::string key = ToLower(distribution.name());
  if (key.empty()) {
    return;
  }
  auto result = distributions_.emplace(std::move(key), std::move(distribution));
  if (!result.second) {
    throw std::runtime_error("duplicate distribution: " + result.first->first);
  }
}

}  // namespace benchgen::tpcds::internal
