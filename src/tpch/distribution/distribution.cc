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

#include "distribution/distribution.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>

namespace benchgen::tpch::internal {
namespace {

std::string ToLowerAscii(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}

bool CaseInsensitiveEquals(const std::string& left, const std::string& right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t i = 0; i < left.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(left[i])) !=
        std::tolower(static_cast<unsigned char>(right[i]))) {
      return false;
    }
  }
  return true;
}

bool CaseInsensitiveStartsWith(const std::string& text, const char* prefix) {
  std::size_t i = 0;
  for (; prefix[i] != '\0'; ++i) {
    if (i >= text.size()) {
      return false;
    }
    if (std::tolower(static_cast<unsigned char>(text[i])) !=
        std::tolower(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
  }
  return true;
}

void StripComments(std::string* line) {
  if (!line) {
    return;
  }
  std::size_t pos = line->find('#');
  if (pos != std::string::npos) {
    line->resize(pos);
  }
  while (!line->empty() && (line->back() == '\n' || line->back() == '\r')) {
    line->pop_back();
  }
}

bool IsBlankLine(const std::string& line) {
  return line.find_first_not_of(" \t") == std::string::npos;
}

}  // namespace

arrow::Status DistributionStore::LoadFromFile(const std::string& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    return arrow::Status::IOError("distribution file not found: ", path);
  }
  return Parse(input);
}

arrow::Status DistributionStore::LoadFromBuffer(const unsigned char* data,
                                                std::size_t size) {
  if (!data || size == 0) {
    return arrow::Status::Invalid("embedded distributions are empty");
  }
  std::string buffer(reinterpret_cast<const char*>(data), size);
  std::istringstream input(buffer);
  return Parse(input);
}

const Distribution* DistributionStore::Find(const std::string& name) const {
  auto it = distributions_.find(ToLowerAscii(name));
  if (it == distributions_.end()) {
    return nullptr;
  }
  return &it->second;
}

arrow::Status DistributionStore::Parse(std::istream& input) {
  distributions_.clear();

  std::string current_name;
  Distribution current_dist;
  int64_t expected_count = -1;
  bool in_dist = false;

  std::string line;
  while (std::getline(input, line)) {
    StripComments(&line);
    if (line.empty() || IsBlankLine(line)) {
      continue;
    }

    if (!in_dist) {
      std::istringstream header(line);
      std::string token;
      std::string name;
      if (!(header >> token >> name)) {
        continue;
      }
      if (!CaseInsensitiveEquals(token, "begin")) {
        continue;
      }
      current_name = ToLowerAscii(name);
      current_dist = Distribution{};
      expected_count = -1;
      in_dist = true;
      continue;
    }

    if (CaseInsensitiveStartsWith(line, "end")) {
      if (expected_count >= 0 &&
          static_cast<int64_t>(current_dist.list.size()) != expected_count) {
        return arrow::Status::Invalid("read error on dist '", current_name,
                                      "'");
      }
      auto inserted =
          distributions_.emplace(current_name, std::move(current_dist));
      if (!inserted.second) {
        return arrow::Status::Invalid("duplicate distribution: ", current_name);
      }
      in_dist = false;
      current_name.clear();
      current_dist = Distribution{};
      expected_count = -1;
      continue;
    }

    std::size_t bar = line.find('|');
    if (bar == std::string::npos) {
      continue;
    }
    std::string token = line.substr(0, bar);
    std::string weight_text = line.substr(bar + 1);

    char* end = nullptr;
    int64_t weight = std::strtoll(weight_text.c_str(), &end, 10);
    if (end == weight_text.c_str()) {
      continue;
    }

    if (CaseInsensitiveEquals(token, "count")) {
      expected_count = weight;
      if (expected_count < 0) {
        return arrow::Status::Invalid("invalid distribution count for ",
                                      current_name);
      }
      current_dist.list.reserve(static_cast<std::size_t>(expected_count));
      continue;
    }

    if (expected_count < 0) {
      return arrow::Status::Invalid("distribution count missing for ",
                                    current_name);
    }
    if (static_cast<int64_t>(current_dist.list.size()) >= expected_count) {
      return arrow::Status::Invalid("distribution entry overflow for ",
                                    current_name);
    }

    current_dist.max += weight;
    DistributionEntry entry;
    entry.text = token;
    entry.weight = current_dist.max;
    current_dist.list.push_back(std::move(entry));
  }

  if (in_dist) {
    return arrow::Status::Invalid("unterminated distribution: ", current_name);
  }

  return arrow::Status::OK();
}

}  // namespace benchgen::tpch::internal
