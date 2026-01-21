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

#include "utils/decimal.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

struct Pricing {
  Decimal wholesale_cost;
  Decimal list_price;
  Decimal sales_price;
  int quantity = 0;
  Decimal ext_discount_amt;
  Decimal ext_sales_price;
  Decimal ext_wholesale_cost;
  Decimal ext_list_price;
  Decimal tax_pct;
  Decimal ext_tax;
  Decimal coupon_amt;
  Decimal ship_cost;
  Decimal ext_ship_cost;
  Decimal net_paid;
  Decimal net_paid_inc_tax;
  Decimal net_paid_inc_ship;
  Decimal net_paid_inc_ship_tax;
  Decimal net_profit;
  Decimal refunded_cash;
  Decimal reversed_charge;
  Decimal store_credit;
  Decimal fee;
  Decimal net_loss;
};

struct PricingLimits {
  int quantity_max = 0;
  Decimal discount_max;
  Decimal markup_max;
  Decimal wholesale_max;
  Decimal coupon_max;
};

struct PricingState {
  int last_id = -1;
  PricingLimits limits;
};

void SetPricing(int pricing_id, Pricing* pricing, RandomNumberStream* stream,
                PricingState* state);
void SetPricing(int pricing_id, Pricing* pricing, RandomNumberStream* stream);

}  // namespace benchgen::tpcds::internal
