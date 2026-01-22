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
// dbgen lineitem row counts at scale 1/5/10 (used for interpolation).
constexpr int64_t kLineItemScale1 = 6001215;
constexpr int64_t kLineItemScale5 = 29999795;
constexpr int64_t kLineItemScale10 = 59986052;

int64_t ScaleLinear(int64_t base, double scale_factor) {
  if (scale_factor < 1.0) {
    int64_t int_scale = static_cast<int64_t>(scale_factor * 1000.0);
    int64_t scaled = (int_scale * base) / 1000;
    return scaled < 1 ? 1 : scaled;
  }
  int64_t scale = static_cast<int64_t>(scale_factor);
  return base * scale;
}

int64_t LineItemCount(double scale_factor) {
  if (scale_factor < 1.0) {
    return ScaleLinear(kLineItemScale1, scale_factor);
  }
  int64_t scale = static_cast<int64_t>(scale_factor);
  if (scale <= 0) {
    return 0;
  }
  int64_t tens = scale / 10;
  int64_t remainder = scale % 10;
  int64_t count = tens * kLineItemScale10;
  if (remainder == 0) {
    return count;
  }
  if (remainder < 5) {
    int64_t delta = kLineItemScale5 - kLineItemScale1;
    count += kLineItemScale1 + (delta * (remainder - 1)) / 4;
    return count;
  }
  if (remainder == 5) {
    return count + kLineItemScale5;
  }
  int64_t delta = kLineItemScale10 - kLineItemScale5;
  count += kLineItemScale5 + (delta * (remainder - 5)) / 5;
  return count;
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
      return LineItemCount(scale_factor);
    case TableId::kNation:
    case TableId::kRegion:
    case TableId::kTableCount:
      break;
  }
  return -1;
}

}  // namespace benchgen::tpch::internal
