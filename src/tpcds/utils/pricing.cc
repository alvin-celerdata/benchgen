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

#include "utils/pricing.h"

#include <cstdlib>
#include <stdexcept>

#include "utils/columns.h"
#include "utils/constants.h"
#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {
namespace {

struct PricingLimitSpec {
  int id;
  const char* quantity;
  const char* markup;
  const char* discount;
  const char* wholesale;
  const char* coupon;
};

constexpr PricingLimitSpec kPricingLimits[] = {
    {CS_PRICING, CS_QUANTITY_MAX, CS_MARKUP_MAX, CS_DISCOUNT_MAX,
     CS_WHOLESALE_MAX, CS_COUPON_MAX},
    {SS_PRICING, SS_QUANTITY_MAX, SS_MARKUP_MAX, SS_DISCOUNT_MAX,
     SS_WHOLESALE_MAX, SS_COUPON_MAX},
    {WS_PRICING, WS_QUANTITY_MAX, WS_MARKUP_MAX, WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX, WS_COUPON_MAX},
    {CR_PRICING, CS_QUANTITY_MAX, CS_MARKUP_MAX, CS_DISCOUNT_MAX,
     CS_WHOLESALE_MAX, CS_COUPON_MAX},
    {SR_PRICING, SS_QUANTITY_MAX, SS_MARKUP_MAX, SS_DISCOUNT_MAX,
     SS_WHOLESALE_MAX, SS_COUPON_MAX},
    {WR_PRICING, WS_QUANTITY_MAX, WS_MARKUP_MAX, WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX, WS_COUPON_MAX},
};

PricingLimits ResolveLimits(int pricing_id, PricingState* state) {
  if (state != nullptr && pricing_id == state->last_id) {
    return state->limits;
  }

  const PricingLimitSpec* spec = nullptr;
  for (const auto& entry : kPricingLimits) {
    if (entry.id == pricing_id) {
      spec = &entry;
      break;
    }
  }
  if (spec == nullptr) {
    throw std::runtime_error("missing pricing limits");
  }

  PricingLimits limits;
  limits.quantity_max = std::atoi(spec->quantity);
  limits.discount_max = DecimalFromString(spec->discount);
  limits.markup_max = DecimalFromString(spec->markup);
  limits.wholesale_max = DecimalFromString(spec->wholesale);
  limits.coupon_max = DecimalFromString(spec->coupon);
  if (state != nullptr) {
    state->last_id = pricing_id;
    state->limits = limits;
  }
  return limits;
}

struct PricingConstants {
  Decimal zero;
  Decimal one_half;
  Decimal nine_pct;
  Decimal one;
  Decimal hundred;
};

const PricingConstants& GetPricingConstants() {
  static PricingConstants constants = {
      DecimalFromString("0.00"),   DecimalFromString("0.50"),
      DecimalFromString("0.09"),   DecimalFromString("1.00"),
      DecimalFromString("100.00"),
  };
  return constants;
}

}  // namespace

void SetPricing(int pricing_id, Pricing* pricing, RandomNumberStream* stream,
                PricingState* state) {
  if (pricing == nullptr || stream == nullptr) {
    throw std::invalid_argument("pricing and stream must not be null");
  }

  PricingLimits limits = ResolveLimits(pricing_id, state);
  const PricingConstants& constants = GetPricingConstants();

  Decimal discount_min = constants.zero;
  Decimal markup_min = constants.zero;
  Decimal wholesale_min = DecimalFromString("1.00");
  Decimal coupon_min = constants.zero;

  switch (pricing_id) {
    case SS_PRICING:
    case CS_PRICING:
    case WS_PRICING: {
      pricing->quantity = GenerateRandomInt(RandomDistribution::kUniform, 1,
                                            limits.quantity_max, 0, stream);
      Decimal d_quantity;
      IntToDecimal(&d_quantity, pricing->quantity);

      pricing->wholesale_cost =
          GenerateRandomDecimal(RandomDistribution::kUniform, wholesale_min,
                                limits.wholesale_max, nullptr, stream);

      ApplyDecimalOp(&pricing->ext_wholesale_cost, DecimalOp::kMultiply,
                     d_quantity, pricing->wholesale_cost);

      Decimal markup =
          GenerateRandomDecimal(RandomDistribution::kUniform, markup_min,
                                limits.markup_max, nullptr, stream);
      Decimal markup_plus_one;
      ApplyDecimalOp(&markup_plus_one, DecimalOp::kAdd, markup, constants.one);
      ApplyDecimalOp(&pricing->list_price, DecimalOp::kMultiply,
                     pricing->wholesale_cost, markup_plus_one);

      Decimal discount =
          GenerateRandomDecimal(RandomDistribution::kUniform, discount_min,
                                limits.discount_max, nullptr, stream);
      Decimal discount_adj = discount;
      NegateDecimal(&discount_adj);
      ApplyDecimalOp(&pricing->ext_discount_amt, DecimalOp::kAdd, discount_adj,
                     constants.one);
      ApplyDecimalOp(&pricing->sales_price, DecimalOp::kMultiply,
                     pricing->list_price, pricing->ext_discount_amt);

      ApplyDecimalOp(&pricing->ext_list_price, DecimalOp::kMultiply,
                     pricing->list_price, d_quantity);
      ApplyDecimalOp(&pricing->ext_sales_price, DecimalOp::kMultiply,
                     pricing->sales_price, d_quantity);
      ApplyDecimalOp(&pricing->ext_discount_amt, DecimalOp::kSubtract,
                     pricing->ext_list_price, pricing->ext_sales_price);

      Decimal coupon =
          GenerateRandomDecimal(RandomDistribution::kUniform, constants.zero,
                                constants.one, nullptr, stream);
      int coupon_usage =
          GenerateRandomInt(RandomDistribution::kUniform, 1, 100, 0, stream);
      if (coupon_usage <= 20) {
        ApplyDecimalOp(&pricing->coupon_amt, DecimalOp::kMultiply,
                       pricing->ext_sales_price, coupon);
      } else {
        pricing->coupon_amt = constants.zero;
      }

      ApplyDecimalOp(&pricing->net_paid, DecimalOp::kSubtract,
                     pricing->ext_sales_price, pricing->coupon_amt);

      Decimal shipping =
          GenerateRandomDecimal(RandomDistribution::kUniform, constants.zero,
                                constants.one_half, nullptr, stream);
      ApplyDecimalOp(&pricing->ship_cost, DecimalOp::kMultiply,
                     pricing->list_price, shipping);
      ApplyDecimalOp(&pricing->ext_ship_cost, DecimalOp::kMultiply,
                     pricing->ship_cost, d_quantity);
      ApplyDecimalOp(&pricing->net_paid_inc_ship, DecimalOp::kAdd,
                     pricing->net_paid, pricing->ext_ship_cost);

      pricing->tax_pct =
          GenerateRandomDecimal(RandomDistribution::kUniform, constants.zero,
                                constants.nine_pct, nullptr, stream);
      ApplyDecimalOp(&pricing->ext_tax, DecimalOp::kMultiply, pricing->net_paid,
                     pricing->tax_pct);
      ApplyDecimalOp(&pricing->net_paid_inc_tax, DecimalOp::kAdd,
                     pricing->net_paid, pricing->ext_tax);
      ApplyDecimalOp(&pricing->net_paid_inc_ship_tax, DecimalOp::kAdd,
                     pricing->net_paid_inc_ship, pricing->ext_tax);
      ApplyDecimalOp(&pricing->net_profit, DecimalOp::kSubtract,
                     pricing->net_paid, pricing->ext_wholesale_cost);
      break;
    }
    case CR_PRICING:
    case SR_PRICING:
    case WR_PRICING: {
      Decimal d_quantity;
      IntToDecimal(&d_quantity, pricing->quantity);
      ApplyDecimalOp(&pricing->ext_wholesale_cost, DecimalOp::kMultiply,
                     d_quantity, pricing->wholesale_cost);
      ApplyDecimalOp(&pricing->ext_list_price, DecimalOp::kMultiply,
                     pricing->list_price, d_quantity);
      ApplyDecimalOp(&pricing->ext_sales_price, DecimalOp::kMultiply,
                     pricing->sales_price, d_quantity);
      pricing->net_paid = pricing->ext_sales_price;

      Decimal shipping =
          GenerateRandomDecimal(RandomDistribution::kUniform, constants.zero,
                                constants.one_half, nullptr, stream);
      ApplyDecimalOp(&pricing->ship_cost, DecimalOp::kMultiply,
                     pricing->list_price, shipping);
      ApplyDecimalOp(&pricing->ext_ship_cost, DecimalOp::kMultiply,
                     pricing->ship_cost, d_quantity);
      ApplyDecimalOp(&pricing->net_paid_inc_ship, DecimalOp::kAdd,
                     pricing->net_paid, pricing->ext_ship_cost);

      ApplyDecimalOp(&pricing->ext_tax, DecimalOp::kMultiply, pricing->net_paid,
                     pricing->tax_pct);
      ApplyDecimalOp(&pricing->net_paid_inc_tax, DecimalOp::kAdd,
                     pricing->net_paid, pricing->ext_tax);
      ApplyDecimalOp(&pricing->net_paid_inc_ship_tax, DecimalOp::kAdd,
                     pricing->net_paid_inc_ship, pricing->ext_tax);
      ApplyDecimalOp(&pricing->net_profit, DecimalOp::kSubtract,
                     pricing->net_paid, pricing->ext_wholesale_cost);

      int cash_pct =
          GenerateRandomInt(RandomDistribution::kUniform, 0, 100, 0, stream);
      Decimal d_temp;
      IntToDecimal(&d_temp, cash_pct);
      ApplyDecimalOp(&pricing->refunded_cash, DecimalOp::kDivide, d_temp,
                     constants.hundred);
      ApplyDecimalOp(&pricing->refunded_cash, DecimalOp::kMultiply,
                     pricing->refunded_cash, pricing->net_paid);

      int credit_pct =
          GenerateRandomInt(RandomDistribution::kUniform, 1, 100, 0, stream);
      Decimal d_temp2;
      IntToDecimal(&d_temp2, credit_pct);
      ApplyDecimalOp(&d_temp, DecimalOp::kDivide, d_temp2, constants.hundred);
      ApplyDecimalOp(&d_temp2, DecimalOp::kSubtract, pricing->net_paid,
                     pricing->refunded_cash);
      ApplyDecimalOp(&pricing->reversed_charge, DecimalOp::kMultiply, d_temp2,
                     d_temp);

      ApplyDecimalOp(&pricing->store_credit, DecimalOp::kSubtract,
                     pricing->net_paid, pricing->reversed_charge);
      ApplyDecimalOp(&pricing->store_credit, DecimalOp::kSubtract,
                     pricing->store_credit, pricing->refunded_cash);

      pricing->fee = GenerateRandomDecimal(
          RandomDistribution::kUniform, constants.one_half, constants.hundred,
          &constants.zero, stream);

      ApplyDecimalOp(&pricing->net_loss, DecimalOp::kSubtract,
                     pricing->net_paid_inc_ship_tax, pricing->store_credit);
      ApplyDecimalOp(&pricing->net_loss, DecimalOp::kSubtract,
                     pricing->net_loss, pricing->refunded_cash);
      ApplyDecimalOp(&pricing->net_loss, DecimalOp::kSubtract,
                     pricing->net_loss, pricing->reversed_charge);
      ApplyDecimalOp(&pricing->net_loss, DecimalOp::kAdd, pricing->net_loss,
                     pricing->fee);
      break;
    }
    default:
      throw std::runtime_error("unsupported pricing id");
  }
}

void SetPricing(int pricing_id, Pricing* pricing, RandomNumberStream* stream) {
  SetPricing(pricing_id, pricing, stream, nullptr);
}

}  // namespace benchgen::tpcds::internal
