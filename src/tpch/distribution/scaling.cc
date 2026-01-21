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

#include "distribution/scaling.h"

#include "utils/constants.h"

namespace benchgen::tpch::internal {
namespace {

constexpr int64_t kPartBase = 200000;
constexpr int64_t kSupplierBase = 10000;
constexpr int64_t kCustomerBase = 150000;
constexpr int64_t kOrdersBase = 150000;

int64_t ScaleLinear(int64_t base, double scale_factor) {
  if (scale_factor < 1.0) {
    int64_t int_scale = static_cast<int64_t>(scale_factor * 1000.0);
    int64_t scaled = (int_scale * base) / 1000;
    return scaled < 1 ? 1 : scaled;
  }
  int64_t scale = static_cast<int64_t>(scale_factor);
  return base * scale;
}

}  // namespace

int64_t OrderCount(double scale_factor) {
  int64_t base = kOrdersBase * kOrdersPerCustomer;
  return ScaleLinear(base, scale_factor);
}

int64_t RowCount(TableId table, double scale_factor) {
  switch (table) {
    case TableId::kPart:
      return ScaleLinear(kPartBase, scale_factor);
    case TableId::kPartSupp:
      return ScaleLinear(kPartBase, scale_factor) * kSuppPerPart;
    case TableId::kSupplier:
      return ScaleLinear(kSupplierBase, scale_factor);
    case TableId::kCustomer:
      return ScaleLinear(kCustomerBase, scale_factor);
    case TableId::kOrders:
      return OrderCount(scale_factor);
    case TableId::kLineItem:
      return -1;
    case TableId::kNation:
    case TableId::kRegion:
    case TableId::kTableCount:
      break;
  }
  return -1;
}

}  // namespace benchgen::tpch::internal
