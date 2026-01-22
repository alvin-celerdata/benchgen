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

#include "utils/scaling.h"

#include <cmath>

namespace benchgen::ssb::internal {
namespace {

constexpr int64_t kCustomerBase = 30000;
constexpr int64_t kSupplierBase = 2000;
constexpr int64_t kPartBase = 200000;
constexpr int64_t kDateBase = 2556;
constexpr int64_t kOrdersBase = 150000;
constexpr int64_t kOrdersPerCustomer = 10;
// dbgen lineorder row counts at scale 1/5/10 (used for interpolation).
constexpr int64_t kLineorderScale1 = 6001215;
constexpr int64_t kLineorderScale5 = 29999795;
constexpr int64_t kLineorderScale10 = 59986052;

int64_t ScaleLinear(int64_t base, double scale_factor) {
  if (scale_factor < 1.0) {
    double scaled = static_cast<double>(base) * scale_factor;
    return scaled < 1.0 ? 1 : static_cast<int64_t>(scaled);
  }
  return base * static_cast<int64_t>(scale_factor);
}

int64_t PartScaleMultiplier(long scale) {
  if (scale <= 1) {
    return 1;
  }
  double factor = 1.0 + (std::log(static_cast<double>(scale)) / std::log(2.0));
  return static_cast<int64_t>(std::floor(factor));
}

int64_t LineorderCount(double scale_factor) {
  if (scale_factor < 1.0) {
    return ScaleLinear(kLineorderScale1, scale_factor);
  }
  int64_t scale = static_cast<int64_t>(scale_factor);
  if (scale <= 0) {
    return 0;
  }
  int64_t tens = scale / 10;
  int64_t remainder = scale % 10;
  int64_t count = tens * kLineorderScale10;
  if (remainder == 0) {
    return count;
  }
  if (remainder < 5) {
    int64_t delta = kLineorderScale5 - kLineorderScale1;
    count += kLineorderScale1 + (delta * (remainder - 1)) / 4;
    return count;
  }
  if (remainder == 5) {
    return count + kLineorderScale5;
  }
  int64_t delta = kLineorderScale10 - kLineorderScale5;
  count += kLineorderScale5 + (delta * (remainder - 5)) / 5;
  return count;
}

}  // namespace

int64_t OrderCount(double scale_factor) {
  long scale = 1;
  if (scale_factor >= 1.0) {
    scale = static_cast<long>(scale_factor);
  }
  int64_t base = kOrdersBase * kOrdersPerCustomer;
  return ScaleLinear(
      base, scale_factor >= 1.0 ? static_cast<double>(scale) : scale_factor);
}

int64_t RowCount(TableId table, double scale_factor) {
  long scale = 1;
  if (scale_factor >= 1.0) {
    scale = static_cast<long>(scale_factor);
  }
  double base_scale = scale_factor < 1.0 ? scale_factor : 1.0;

  switch (table) {
    case TableId::kCustomer:
      return ScaleLinear(kCustomerBase,
                         scale_factor >= 1.0 ? scale : scale_factor);
    case TableId::kSupplier:
      return ScaleLinear(kSupplierBase,
                         scale_factor >= 1.0 ? scale : scale_factor);
    case TableId::kPart: {
      int64_t multiplier = PartScaleMultiplier(scale);
      int64_t base = kPartBase * multiplier;
      double scaled = static_cast<double>(base) * base_scale;
      return scaled < 1.0 ? 1 : static_cast<int64_t>(scaled);
    }
    case TableId::kDate: {
      double scaled = static_cast<double>(kDateBase) * base_scale;
      return scaled < 1.0 ? 1 : static_cast<int64_t>(scaled);
    }
    case TableId::kLineorder:
      return LineorderCount(scale_factor);
    case TableId::kTableCount:
      break;
  }
  return -1;
}

}  // namespace benchgen::ssb::internal
