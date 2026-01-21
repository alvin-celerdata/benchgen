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
#include <string_view>

namespace benchgen::tpcds::internal {

struct Decimal {
  int scale = 0;
  int precision = 0;
  int64_t number = 0;
};

enum class DecimalOp {
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
};

void SetPrecision(Decimal* dest, int scale, int precision);
void IntToDecimal(Decimal* dest, int value);
void StringToDecimal(Decimal* dest, std::string_view input);
Decimal DecimalFromString(std::string_view input);
void ApplyDecimalOp(Decimal* dest, DecimalOp op, const Decimal& left,
                    const Decimal& right);
void NegateDecimal(Decimal* value);

}  // namespace benchgen::tpcds::internal
