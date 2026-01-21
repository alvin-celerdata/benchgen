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

#include "benchgen/table.h"

#include <array>
#include <cctype>
#include <string>

namespace {

template <typename TableIdEnum, size_t N>
std::string_view TableIdToStringImpl(
    TableIdEnum table, const std::array<std::string_view, N>& names) {
  auto index = static_cast<size_t>(table);
  if (index >= names.size()) {
    return "unknown";
  }
  return names[index];
}

template <typename TableIdEnum, size_t N>
bool TableIdFromStringImpl(std::string_view normalized, TableIdEnum* out,
                           const std::array<std::string_view, N>& names) {
  for (size_t i = 0; i < names.size(); ++i) {
    if (normalized == names[i]) {
      *out = static_cast<TableIdEnum>(i);
      return true;
    }
  }
  return false;
}

std::string NormalizeTableNameDropSeparators(std::string_view name) {
  std::string normalized;
  normalized.reserve(name.size());
  for (char c : name) {
    char lowered =
        static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (lowered == '-' || lowered == '_') {
      continue;
    }
    normalized.push_back(lowered);
  }
  return normalized;
}

std::string NormalizeTableNameUnderscore(std::string_view name) {
  std::string normalized;
  normalized.reserve(name.size());
  for (char c : name) {
    char lowered =
        static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (lowered == '-') {
      lowered = '_';
    }
    normalized.push_back(lowered);
  }
  return normalized;
}

}  // namespace

namespace benchgen::tpch {
namespace {

constexpr std::array<std::string_view,
                     static_cast<size_t>(TableId::kTableCount)>
    kTableNames = {
        "part",   "partsupp", "supplier", "customer",
        "orders", "lineitem", "nation",   "region",
};

}  // namespace

std::string_view TableIdToString(TableId table) {
  return TableIdToStringImpl(table, kTableNames);
}

bool TableIdFromString(std::string_view name, TableId* out) {
  if (out == nullptr) {
    return false;
  }
  std::string normalized = NormalizeTableNameDropSeparators(name);
  return TableIdFromStringImpl(normalized, out, kTableNames);
}

}  // namespace benchgen::tpch

namespace benchgen::tpcds {
namespace {

constexpr std::array<std::string_view,
                     static_cast<size_t>(TableId::kTableCount)>
    kTableNames = {
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
};

}  // namespace

std::string_view TableIdToString(TableId table) {
  return TableIdToStringImpl(table, kTableNames);
}

bool TableIdFromString(std::string_view name, TableId* out) {
  if (out == nullptr) {
    return false;
  }
  std::string normalized = NormalizeTableNameUnderscore(name);
  return TableIdFromStringImpl(normalized, out, kTableNames);
}

}  // namespace benchgen::tpcds

namespace benchgen::ssb {
namespace {

constexpr std::array<std::string_view,
                     static_cast<size_t>(TableId::kTableCount)>
    kTableNames = {
        "customer", "part", "supplier", "date", "lineorder",
};

}  // namespace

std::string_view TableIdToString(TableId table) {
  return TableIdToStringImpl(table, kTableNames);
}

bool TableIdFromString(std::string_view name, TableId* out) {
  if (out == nullptr) {
    return false;
  }
  std::string normalized = NormalizeTableNameUnderscore(name);
  if (TableIdFromStringImpl(normalized, out, kTableNames)) {
    return true;
  }
  if (normalized == "line_order") {
    *out = TableId::kLineorder;
    return true;
  }
  return false;
}

}  // namespace benchgen::ssb
