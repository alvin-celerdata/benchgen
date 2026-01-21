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
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

constexpr int32_t kTknVarchar = 6;
constexpr int32_t kTknInt = 7;
constexpr int32_t kTknDate = 9;
constexpr int32_t kTknDecimal = 10;
constexpr int32_t kIdxNameLength = 20;

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

std::string_view TrimLeading(std::string_view input) {
  size_t start = 0;
  while (start < input.size()) {
    char c = input[start];
    if (c != ' ' && c != '\t' && c != '\r') {
      break;
    }
    ++start;
  }
  return input.substr(start);
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

std::string StripComments(std::string_view line) {
  size_t dash = line.find('-');
  if (dash != std::string::npos && dash + 1 < line.size() &&
      line[dash + 1] == '-') {
    return std::string(line.substr(0, dash));
  }
  return std::string(line);
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
      return trimmed.substr(start + 1, end - start - 1);
    }
  }
  size_t end = trimmed.find_first_of(" \t;");
  if (end == std::string::npos) {
    end = trimmed.size();
  }
  return trimmed.substr(start, end - start);
}

std::vector<std::string> SplitTokens(std::string_view input) {
  std::vector<std::string> tokens;
  std::string current;
  for (char c : input) {
    if (std::isspace(static_cast<unsigned char>(c)) || c == '(' || c == ')' ||
        c == '=' || c == ',' || c == ';' || c == ':') {
      if (!current.empty()) {
        tokens.push_back(NormalizeValueEncoding(current));
        current.clear();
      }
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    tokens.push_back(NormalizeValueEncoding(current));
  }
  return tokens;
}

std::vector<int32_t> ParseTypes(std::string_view input) {
  std::vector<int32_t> types;
  std::string current;
  for (char c : input) {
    if (std::isspace(static_cast<unsigned char>(c)) || c == ',') {
      if (!current.empty()) {
        std::string token = ToLower(current);
        if (token == "varchar" || token == "char") {
          types.push_back(kTknVarchar);
        } else if (token == "int" || token == "integer") {
          types.push_back(kTknInt);
        } else if (token == "date") {
          types.push_back(kTknDate);
        } else if (token == "decimal") {
          types.push_back(kTknDecimal);
        } else {
          throw std::runtime_error("unknown distribution type: " + current);
        }
        current.clear();
      }
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    std::string token = ToLower(current);
    if (token == "varchar" || token == "char") {
      types.push_back(kTknVarchar);
    } else if (token == "int" || token == "integer") {
      types.push_back(kTknInt);
    } else if (token == "date") {
      types.push_back(kTknDate);
    } else if (token == "decimal") {
      types.push_back(kTknDecimal);
    } else {
      throw std::runtime_error("unknown distribution type: " + current);
    }
  }
  return types;
}

char* NextToken(char** input, const char* delims) {
  if (input == nullptr || *input == nullptr) {
    return nullptr;
  }
  char* start = *input;
  while (*start && std::strchr(delims, *start) != nullptr) {
    ++start;
  }
  if (*start == '\0') {
    *input = nullptr;
    return nullptr;
  }
  char* end = start;
  while (*end && std::strchr(delims, *end) == nullptr) {
    ++end;
  }
  if (*end == '\0') {
    *input = nullptr;
  } else {
    *end = '\0';
    *input = end + 1;
  }
  return start;
}

struct Distribution {
  std::string name;
  std::vector<int32_t> types;
  std::vector<std::vector<std::string>> values;
  std::vector<std::vector<int32_t>> weights;
  std::vector<std::string> value_names;
  std::vector<std::string> weight_names;
  int32_t length = 0;

  void SetTypes(std::vector<int32_t> input_types) {
    types = std::move(input_types);
    values.assign(types.size(), {});
  }

  void SetWeightSetCount(int32_t count) {
    if (count < 0) {
      throw std::runtime_error("weight set count must be non-negative");
    }
    weights.assign(static_cast<size_t>(count), {});
  }

  void SetNames(std::vector<std::string> values_in,
                std::vector<std::string> weights_in) {
    value_names = std::move(values_in);
    weight_names = std::move(weights_in);
  }

  int32_t value_set_count() const { return static_cast<int32_t>(types.size()); }

  int32_t weight_set_count() const {
    return static_cast<int32_t>(weights.size());
  }

  void AddEntry(const std::vector<std::string>& entry_values,
                const std::vector<int32_t>& entry_weights) {
    if (value_set_count() > 0 &&
        static_cast<int32_t>(entry_values.size()) != value_set_count()) {
      throw std::runtime_error("distribution value count mismatch for " + name);
    }
    if (weight_set_count() > 0 &&
        static_cast<int32_t>(entry_weights.size()) != weight_set_count()) {
      throw std::runtime_error("distribution weight count mismatch for " +
                               name);
    }
    if (values.empty()) {
      values.assign(entry_values.size(), {});
    }
    for (size_t i = 0; i < entry_values.size(); ++i) {
      values[i].push_back(entry_values[i]);
    }
    for (size_t i = 0; i < entry_weights.size(); ++i) {
      weights[i].push_back(entry_weights[i]);
    }
    ++length;
  }
};

struct ParserState {
  std::vector<Distribution> distributions;
  std::unordered_set<std::string> loaded_files;
  std::unordered_set<std::string> names_lower;
  Distribution current;
  bool has_current = false;
};

void AddDistribution(ParserState* state, Distribution dist) {
  std::string key = ToLower(dist.name);
  if (key.empty()) {
    return;
  }
  if (!state->names_lower.insert(key).second) {
    throw std::runtime_error("duplicate distribution: " + dist.name);
  }
  state->distributions.push_back(std::move(dist));
}

void ParseAddStatement(const std::string& stmt, Distribution* dist) {
  if (dist->value_set_count() == 0 || dist->weight_set_count() == 0) {
    throw std::runtime_error("add entry without types/weights in " +
                             dist->name);
  }

  std::vector<std::string> values;
  values.reserve(static_cast<size_t>(dist->value_set_count()));

  const char* cursor = stmt.c_str();
  for (int32_t i = 0; i < dist->value_set_count(); ++i) {
    int32_t type = dist->types[static_cast<size_t>(i)];
    if (type == kTknVarchar) {
      while (*cursor && *cursor != '"') {
        ++cursor;
      }
      if (*cursor == '\0') {
        throw std::runtime_error("invalid add line in " + dist->name);
      }
      const char* start = cursor + 1;
      cursor = start;
      while (*cursor && *cursor != '"') {
        ++cursor;
      }
      if (*cursor == '\0') {
        throw std::runtime_error("invalid add line in " + dist->name);
      }
      values.push_back(NormalizeValueEncoding(
          std::string_view(start, static_cast<size_t>(cursor - start))));
      ++cursor;
    } else {
      while (*cursor && !std::isdigit(static_cast<unsigned char>(*cursor)) &&
             *cursor != '-') {
        ++cursor;
      }
      if (*cursor == '\0') {
        throw std::runtime_error("invalid add line in " + dist->name);
      }
      const char* start = cursor;
      while (*cursor && (std::isdigit(static_cast<unsigned char>(*cursor)) ||
                         *cursor == '-')) {
        ++cursor;
      }
      values.push_back(NormalizeValueEncoding(
          std::string_view(start, static_cast<size_t>(cursor - start))));
    }
  }

  std::vector<int32_t> weights;
  weights.reserve(static_cast<size_t>(dist->weight_set_count()));
  std::string weight_text = cursor ? std::string(cursor) : std::string();
  char* weight_cursor = weight_text.empty() ? nullptr : weight_text.data();
  for (int32_t i = 0; i < dist->weight_set_count(); ++i) {
    char* token = NextToken(&weight_cursor, ":) \t,");
    int32_t weight = token ? static_cast<int32_t>(std::atoi(token)) : 0;
    weights.push_back(weight);
  }

  dist->AddEntry(values, weights);
}

void LoadFile(const std::string& path, ParserState* state, int depth) {
  std::string normalized = path;
  if (!state->loaded_files.insert(normalized).second) {
    return;
  }

  std::ifstream file(path);
  if (!file.is_open()) {
    throw std::runtime_error("unable to open distribution file: " + path);
  }

  const std::string directory = DirectoryFromPath(path);
  std::string pending;
  std::string line;
  while (std::getline(file, line)) {
    std::string cleaned = StripComments(line);
    std::string_view line_start = TrimLeading(cleaned);
    if (line_start.empty()) {
      continue;
    }

    pending.append(line_start.begin(), line_start.end());

    size_t semi = line_start.find(';');
    if (semi == std::string_view::npos) {
      pending.push_back(' ');
      continue;
    }
    if (semi > 0 && line_start[semi - 1] == '\\') {
      size_t next = line_start.find(';', semi + 1);
      if (next == std::string_view::npos) {
        pending.push_back(' ');
        continue;
      }
    }

    std::string statement = Trim(pending);
    pending.clear();
    if (statement.empty()) {
      continue;
    }

    if (StartsWithIgnoreCase(statement, "#include")) {
      std::string include_path =
          TrimInclude(std::string_view(statement).substr(8));
      if (!include_path.empty()) {
        if (!directory.empty() && include_path.front() != '/' &&
            include_path.front() != '\\') {
          include_path = directory + "/" + include_path;
        }
        LoadFile(include_path, state, depth + 1);
      }
      continue;
    }

    if (StartsWithIgnoreCase(statement, "create ")) {
      if (state->has_current) {
        AddDistribution(state, std::move(state->current));
        state->current = Distribution();
      }
      std::string name = Trim(statement.substr(6));
      if (!name.empty() && name.back() == ';') {
        name.pop_back();
      }
      state->current.name = Trim(name);
      state->has_current = true;
      continue;
    }

    if (!state->has_current) {
      continue;
    }

    if (StartsWithIgnoreCase(statement, "set types")) {
      size_t open = statement.find('(');
      size_t close = statement.rfind(')');
      if (open == std::string::npos || close == std::string::npos ||
          close <= open) {
        throw std::runtime_error("invalid types line in " + path);
      }
      state->current.SetTypes(ParseTypes(
          std::string_view(statement).substr(open + 1, close - open - 1)));
      continue;
    }

    if (StartsWithIgnoreCase(statement, "set weights")) {
      size_t eq = statement.find('=');
      if (eq == std::string::npos) {
        throw std::runtime_error("invalid weights line in " + path);
      }
      std::string count = Trim(statement.substr(eq + 1));
      if (!count.empty() && count.back() == ';') {
        count.pop_back();
      }
      state->current.SetWeightSetCount(std::stoi(count));
      continue;
    }

    if (StartsWithIgnoreCase(statement, "set names")) {
      size_t open = statement.find('(');
      size_t close = statement.rfind(')');
      if (open == std::string::npos || close == std::string::npos ||
          close <= open) {
        throw std::runtime_error("invalid names line in " + path);
      }
      std::vector<std::string> tokens = SplitTokens(
          std::string_view(statement).substr(open + 1, close - open - 1));
      std::vector<std::string> value_names;
      std::vector<std::string> weight_names;
      if (static_cast<int32_t>(tokens.size()) >=
          state->current.value_set_count()) {
        value_names.assign(tokens.begin(),
                           tokens.begin() + state->current.value_set_count());
        weight_names.assign(tokens.begin() + state->current.value_set_count(),
                            tokens.end());
      } else {
        value_names = std::move(tokens);
      }
      state->current.SetNames(std::move(value_names), std::move(weight_names));
      continue;
    }

    if (StartsWithIgnoreCase(statement, "add")) {
      ParseAddStatement(statement, &state->current);
      continue;
    }
  }

  if (depth == 0 && state->has_current) {
    AddDistribution(state, std::move(state->current));
    state->current = Distribution();
    state->has_current = false;
  }
}

void WriteBE32(std::ofstream* out, int32_t value) {
  uint32_t v = static_cast<uint32_t>(value);
  unsigned char buffer[4] = {
      static_cast<unsigned char>((v >> 24) & 0xFF),
      static_cast<unsigned char>((v >> 16) & 0xFF),
      static_cast<unsigned char>((v >> 8) & 0xFF),
      static_cast<unsigned char>(v & 0xFF),
  };
  out->write(reinterpret_cast<const char*>(buffer), sizeof(buffer));
}

struct IdxEntry {
  std::string name;
  int32_t index = 0;
  int32_t offset = 0;
  int32_t str_space = 0;
  int32_t length = 0;
  int32_t w_width = 0;
  int32_t v_width = 0;
  int32_t name_space = 0;
};

void WriteIdx(const std::vector<Distribution>& distributions,
              const std::string& output_path) {
  std::filesystem::path out_path(output_path);
  if (out_path.has_parent_path()) {
    std::filesystem::create_directories(out_path.parent_path());
  }

  std::ofstream out(output_path, std::ios::binary);
  if (!out.is_open()) {
    throw std::runtime_error("unable to open output file: " + output_path);
  }

  WriteBE32(&out, static_cast<int32_t>(distributions.size()));
  std::vector<IdxEntry> entries;
  entries.reserve(distributions.size());

  for (size_t idx = 0; idx < distributions.size(); ++idx) {
    const Distribution& dist = distributions[idx];
    IdxEntry entry;
    entry.name = dist.name;
    entry.index = static_cast<int32_t>(idx + 1);
    entry.offset = static_cast<int32_t>(out.tellp());
    entry.length = dist.length;
    entry.w_width = dist.weight_set_count();
    entry.v_width = dist.value_set_count();

    for (int32_t type : dist.types) {
      WriteBE32(&out, type);
    }

    for (const auto& weight_set : dist.weights) {
      for (int32_t weight : weight_set) {
        WriteBE32(&out, weight);
      }
    }

    std::vector<std::vector<int32_t>> offsets;
    offsets.resize(static_cast<size_t>(entry.v_width));
    for (int32_t v = 0; v < entry.v_width; ++v) {
      offsets[static_cast<size_t>(v)].assign(static_cast<size_t>(entry.length),
                                             0);
    }
    std::string strings;
    strings.reserve(static_cast<size_t>(entry.length * entry.v_width * 8));
    int32_t str_offset = 0;
    for (int32_t row = 0; row < entry.length; ++row) {
      for (int32_t v = 0; v < entry.v_width; ++v) {
        const std::string& value =
            dist.values[static_cast<size_t>(v)].at(static_cast<size_t>(row));
        offsets[static_cast<size_t>(v)].at(static_cast<size_t>(row)) =
            str_offset;
        strings.append(value);
        strings.push_back('\0');
        str_offset += static_cast<int32_t>(value.size() + 1);
      }
    }
    entry.str_space = str_offset;

    for (const auto& offset_set : offsets) {
      for (int32_t offset : offset_set) {
        WriteBE32(&out, offset);
      }
    }

    std::string names;
    if (!dist.value_names.empty() || !dist.weight_names.empty()) {
      for (const auto& name : dist.value_names) {
        names.append(name);
        names.push_back('\0');
      }
      for (const auto& name : dist.weight_names) {
        names.append(name);
        names.push_back('\0');
      }
    }
    entry.name_space = static_cast<int32_t>(names.size());
    if (!names.empty()) {
      out.write(names.data(), names.size());
    }
    if (!strings.empty()) {
      out.write(strings.data(), strings.size());
    }

    entries.push_back(std::move(entry));
  }

  for (const auto& entry : entries) {
    std::string name = entry.name;
    if (name.size() > static_cast<size_t>(kIdxNameLength)) {
      name.resize(static_cast<size_t>(kIdxNameLength));
      std::cerr << "warning: truncated distribution name " << entry.name
                << "\n";
    }
    std::string name_buffer =
        name +
        std::string(static_cast<size_t>(kIdxNameLength - name.size()), '\0');
    out.write(name_buffer.data(), name_buffer.size());
    WriteBE32(&out, entry.index);
    WriteBE32(&out, entry.offset);
    WriteBE32(&out, entry.str_space);
    WriteBE32(&out, entry.length);
    WriteBE32(&out, entry.w_width);
    WriteBE32(&out, entry.v_width);
    WriteBE32(&out, entry.name_space);
  }
}

class Md5 {
 public:
  Md5();

  void Update(const void* data, size_t length);
  std::string Final();

 private:
  void Transform(const uint8_t block[64]);

  uint64_t bit_count_ = 0;
  uint32_t state_[4];
  uint8_t buffer_[64];
  bool finalized_ = false;
};

constexpr std::array<uint32_t, 64> kK = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a,
    0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340,
    0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
    0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92,
    0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
};

constexpr std::array<uint32_t, 64> kS = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5, 9,  14, 20, 5, 9,  14, 20, 5, 9,  14, 20, 5, 9,  14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
};

inline uint32_t LeftRotate(uint32_t x, uint32_t c) {
  return (x << c) | (x >> (32 - c));
}

void Encode(uint8_t* output, const uint32_t* input, size_t length) {
  for (size_t i = 0, j = 0; j < length; ++i, j += 4) {
    output[j] = static_cast<uint8_t>(input[i] & 0xff);
    output[j + 1] = static_cast<uint8_t>((input[i] >> 8) & 0xff);
    output[j + 2] = static_cast<uint8_t>((input[i] >> 16) & 0xff);
    output[j + 3] = static_cast<uint8_t>((input[i] >> 24) & 0xff);
  }
}

void Decode(uint32_t* output, const uint8_t* input, size_t length) {
  for (size_t i = 0, j = 0; j < length; ++i, j += 4) {
    output[i] = static_cast<uint32_t>(input[j]) |
                (static_cast<uint32_t>(input[j + 1]) << 8) |
                (static_cast<uint32_t>(input[j + 2]) << 16) |
                (static_cast<uint32_t>(input[j + 3]) << 24);
  }
}

Md5::Md5() {
  state_[0] = 0x67452301;
  state_[1] = 0xefcdab89;
  state_[2] = 0x98badcfe;
  state_[3] = 0x10325476;
  std::memset(buffer_, 0, sizeof(buffer_));
}

void Md5::Update(const void* data, size_t length) {
  if (finalized_) {
    return;
  }

  const uint8_t* input = static_cast<const uint8_t*>(data);
  size_t index = static_cast<size_t>((bit_count_ / 8) % 64);
  bit_count_ += static_cast<uint64_t>(length) * 8;

  size_t part_len = 64 - index;
  size_t i = 0;
  if (length >= part_len) {
    std::memcpy(&buffer_[index], input, part_len);
    Transform(buffer_);
    for (i = part_len; i + 63 < length; i += 64) {
      Transform(&input[i]);
    }
    index = 0;
  }
  std::memcpy(&buffer_[index], &input[i], length - i);
}

std::string Md5::Final() {
  if (finalized_) {
    return "";
  }

  uint8_t padding[64] = {0x80};
  uint8_t length_bytes[8];
  uint32_t count[2] = {
      static_cast<uint32_t>(bit_count_ & 0xffffffff),
      static_cast<uint32_t>((bit_count_ >> 32) & 0xffffffff),
  };
  Encode(length_bytes, count, 8);

  size_t index = static_cast<size_t>((bit_count_ / 8) % 64);
  size_t pad_len = (index < 56) ? (56 - index) : (120 - index);
  Update(padding, pad_len);
  Update(length_bytes, 8);

  uint8_t digest[16];
  Encode(digest, state_, 16);

  std::ostringstream out;
  out.setf(std::ios::hex, std::ios::basefield);
  out.fill('0');
  for (uint8_t byte : digest) {
    out.width(2);
    out << static_cast<int>(byte);
  }

  finalized_ = true;
  return out.str();
}

void Md5::Transform(const uint8_t block[64]) {
  uint32_t a = state_[0];
  uint32_t b = state_[1];
  uint32_t c = state_[2];
  uint32_t d = state_[3];
  uint32_t x[16];
  Decode(x, block, 64);

  for (uint32_t i = 0; i < 64; ++i) {
    uint32_t f = 0;
    uint32_t g = 0;
    if (i < 16) {
      f = (b & c) | (~b & d);
      g = i;
    } else if (i < 32) {
      f = (d & b) | (~d & c);
      g = (5 * i + 1) % 16;
    } else if (i < 48) {
      f = b ^ c ^ d;
      g = (3 * i + 5) % 16;
    } else {
      f = c ^ (b | ~d);
      g = (7 * i) % 16;
    }
    uint32_t temp = d;
    d = c;
    c = b;
    b = b + LeftRotate(a + f + kK[i] + x[g], kS[i]);
    a = temp;
  }

  state_[0] += a;
  state_[1] += b;
  state_[2] += c;
  state_[3] += d;

  std::memset(x, 0, sizeof(x));
}

std::string Md5File(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("unable to open file for md5: " + path);
  }
  Md5 md5;
  char buffer[8192];
  while (file) {
    file.read(buffer, sizeof(buffer));
    std::streamsize count = file.gcount();
    if (count > 0) {
      md5.Update(buffer, static_cast<size_t>(count));
    }
  }
  return md5.Final();
}

void PrintUsage(const char* argv0) {
  std::cerr << "Usage: " << argv0
            << " [--input FILE] [--output FILE]"
               " [--compare FILE]\n";
}

}  // namespace

int main(int argc, char** argv) {
#ifdef TPCDS_RESOURCE_DISTRIBUTION_DIR
  std::string distribution_dir = TPCDS_RESOURCE_DISTRIBUTION_DIR;
#else
  std::string distribution_dir = "resources/tpcds/distribution";
#endif
  std::string input = "tpcds.dst";
  std::string output = "generated/tpcds.idx";
  std::string compare;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--input" && i + 1 < argc) {
      input = argv[++i];
    } else if (arg == "--output" && i + 1 < argc) {
      output = argv[++i];
    } else if (arg == "--compare" && i + 1 < argc) {
      compare = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return 0;
    } else {
      PrintUsage(argv[0]);
      return 1;
    }
  }

  std::string root_path = input;
  if (!root_path.empty() && root_path.front() != '/' &&
      root_path.front() != '\\') {
    root_path = distribution_dir + "/" + root_path;
  }

  try {
    ParserState state;
    LoadFile(root_path, &state, 0);
    if (state.distributions.empty()) {
      std::cerr << "no distributions found\n";
      return 1;
    }

    WriteIdx(state.distributions, output);
    std::cout << "Wrote " << output << "\n";

    if (!compare.empty()) {
      std::string generated_md5 = Md5File(output);
      std::string compare_md5 = Md5File(compare);
      std::cout << "MD5 generated: " << generated_md5 << "\n";
      std::cout << "MD5 compare:   " << compare_md5 << "\n";
      if (generated_md5 != compare_md5) {
        std::cerr << "MD5 mismatch\n";
        return 1;
      }
      std::cout << "MD5 match\n";
    }
  } catch (const std::exception& exc) {
    std::cerr << exc.what() << "\n";
    return 1;
  }

  return 0;
}
