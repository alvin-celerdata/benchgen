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

#include <cstdint>

#include "utils/tables.h"

namespace benchgen::tpcds::internal {

struct TableMetadata {
  int table_number = 0;
  int first_column = 0;
  int last_column = 0;
  int flags = 0;
  int null_pct = 0;
  int64_t not_null_bitmap = 0;
  int param = 0;
};

constexpr int kFlagDateBased = 0x0002;
constexpr int kFlagChild = 0x0004;
constexpr int kFlagType2 = 0x0020;
constexpr int kFlagSmall = 0x0040;
constexpr int kFlagSparse = 0x0080;
constexpr int kFlagParent = 0x1000;
constexpr int kFlagVPrint = 0x8000;

const TableMetadata& GetTableMetadata(int table_number);
int TableFromColumn(int column_id);
bool IsType2Table(int table_number);
bool IsSmallTable(int table_number);
bool IsSparseTable(int table_number);
bool IsParentTable(int table_number);
bool IsDateBasedTable(int table_number);

}  // namespace benchgen::tpcds::internal
