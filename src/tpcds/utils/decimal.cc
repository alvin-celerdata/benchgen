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

#include "utils/decimal.h"

#include <algorithm>
#include <cstdlib>
#include <string>

namespace benchgen::tpcds::internal {

void SetPrecision(Decimal* dest, int scale, int precision) {
  if (dest == nullptr) {
    return;
  }
  dest->scale = scale;
  dest->precision = precision;
  dest->number = 0;
}

void IntToDecimal(Decimal* dest, int value) {
  if (dest == nullptr) {
    return;
  }
  int scale = 1;
  int bound = 1;
  while ((bound * 10) <= value) {
    scale += 1;
    bound *= 10;
  }
  dest->precision = 0;
  dest->scale = scale;
  dest->number = value;
}

void StringToDecimal(Decimal* dest, std::string_view input) {
  if (dest == nullptr) {
    return;
  }
  std::string value(input);
  dest->precision = 0;
  dest->scale = 0;
  dest->number = 0;

  std::string::size_type dot = value.find('.');
  if (dot == std::string::npos) {
    dest->scale = static_cast<int>(value.size());
    dest->number = std::strtoll(value.c_str(), nullptr, 10);
    return;
  }

  std::string integer_part = value.substr(0, dot);
  std::string fraction_part = value.substr(dot + 1);
  dest->scale = static_cast<int>(integer_part.size());
  dest->number = std::strtoll(integer_part.c_str(), nullptr, 10);
  dest->precision = static_cast<int>(fraction_part.size());
  for (int i = 0; i < dest->precision; ++i) {
    dest->number *= 10;
  }
  if (!fraction_part.empty()) {
    dest->number += std::strtoll(fraction_part.c_str(), nullptr, 10);
  }
  if (!value.empty() && value[0] == '-' && dest->number > 0) {
    dest->number *= -1;
  }
}

Decimal DecimalFromString(std::string_view input) {
  Decimal out;
  StringToDecimal(&out, input);
  return out;
}

void ApplyDecimalOp(Decimal* dest, DecimalOp op, const Decimal& left,
                    const Decimal& right) {
  if (dest == nullptr) {
    return;
  }

  dest->scale = std::max(left.scale, right.scale);
  dest->precision = std::max(left.precision, right.precision);

  switch (op) {
    case DecimalOp::kAdd:
      dest->number = left.number + right.number;
      break;
    case DecimalOp::kSubtract:
      dest->number = left.number - right.number;
      break;
    case DecimalOp::kMultiply: {
      int res = left.precision + right.precision;
      dest->number = left.number * right.number;
      while (res-- > dest->precision) {
        dest->number /= 10;
      }
      break;
    }
    case DecimalOp::kDivide: {
      double f1 = static_cast<double>(left.number);
      int np = left.precision;
      while (np < dest->precision) {
        f1 *= 10.0;
        np += 1;
      }
      np = 0;
      while (np < dest->precision) {
        f1 *= 10.0;
        np += 1;
      }
      double f2 = static_cast<double>(right.number);
      np = right.precision;
      while (np < dest->precision) {
        f2 *= 10.0;
        np += 1;
      }
      dest->number = static_cast<int64_t>(f1 / f2);
      break;
    }
  }
}

void NegateDecimal(Decimal* value) {
  if (value != nullptr) {
    value->number *= -1;
  }
}

}  // namespace benchgen::tpcds::internal
