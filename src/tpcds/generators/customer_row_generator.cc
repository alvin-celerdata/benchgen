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

#include "generators/customer_row_generator.h"

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {
namespace {

constexpr Date kTodaysDate{2003, 1, 8};
constexpr Date kBirthMin{1924, 1, 1};
constexpr Date kBirthMax{1992, 12, 31};

}  // namespace

const std::array<CustomerColumnInfo,
                 static_cast<size_t>(CustomerGeneratorColumn::kCount)>&
CustomerColumnInfos() {
  static const std::array<CustomerColumnInfo,
                          static_cast<size_t>(CustomerGeneratorColumn::kCount)>
      kInfos = {{
          {CustomerGeneratorColumn::kCustomerSk, 114, 1},
          {CustomerGeneratorColumn::kCustomerId, 115, 1},
          {CustomerGeneratorColumn::kCurrentCdemoSk, 116, 1},
          {CustomerGeneratorColumn::kCurrentHdemoSk, 117, 1},
          {CustomerGeneratorColumn::kCurrentAddrSk, 118, 1},
          {CustomerGeneratorColumn::kFirstShiptoDateId, 119, 0},
          {CustomerGeneratorColumn::kFirstSalesDateId, 120, 1},
          {CustomerGeneratorColumn::kSalutation, 121, 1},
          {CustomerGeneratorColumn::kFirstName, 122, 1},
          {CustomerGeneratorColumn::kLastName, 123, 1},
          {CustomerGeneratorColumn::kPreferredCustFlag, 124, 2},
          {CustomerGeneratorColumn::kBirthDay, 125, 1},
          {CustomerGeneratorColumn::kBirthMonth, 126, 0},
          {CustomerGeneratorColumn::kBirthYear, 127, 0},
          {CustomerGeneratorColumn::kBirthCountry, 128, 1},
          {CustomerGeneratorColumn::kLogin, 129, 1},
          {CustomerGeneratorColumn::kEmailAddress, 130, 23},
          {CustomerGeneratorColumn::kLastReviewDate, 131, 1},
          {CustomerGeneratorColumn::kNulls, 132, 2},
      }};
  return kInfos;
}

CustomerRowGenerator::CustomerRowGenerator(double scale)
    : scaling_(scale), distributions_() {
  const auto& infos = CustomerColumnInfos();
  streams_.reserve(infos.size());
  for (const auto& info : infos) {
    streams_.emplace_back(info.global_column_number, info.seeds_per_row);
  }
  today_ = kTodaysDate;
  one_year_ago_ = Date::FromJulianDays(Date::ToJulianDays(today_) - 365);
  ten_years_ago_ = Date::FromJulianDays(Date::ToJulianDays(today_) - 3650);
}

void CustomerRowGenerator::SkipRows(int64_t start_row) {
  for (auto& stream : streams_) {
    stream.SkipRows(start_row);
  }
}

CustomerRowData CustomerRowGenerator::GenerateRow(int64_t row_number) {
  CustomerRowData row;
  row.customer_sk = row_number;
  row.customer_id = MakeBusinessKey(static_cast<uint64_t>(row_number));

  int pref = GenerateUniformRandomInt(
      1, 100, &Stream(CustomerGeneratorColumn::kPreferredCustFlag));
  row.preferred_cust_flag = pref < kPreferredPct;

  row.current_hdemo_sk = GenerateUniformRandomKey(
      1, scaling_.RowCount(benchgen::tpcds::TableId::kHouseholdDemographics),
      &Stream(CustomerGeneratorColumn::kCurrentHdemoSk));
  row.current_cdemo_sk = GenerateUniformRandomKey(
      1, scaling_.RowCount(benchgen::tpcds::TableId::kCustomerDemographics),
      &Stream(CustomerGeneratorColumn::kCurrentCdemoSk));
  row.current_addr_sk = GenerateUniformRandomKey(
      1, scaling_.RowCount(benchgen::tpcds::TableId::kCustomerAddress),
      &Stream(CustomerGeneratorColumn::kCurrentAddrSk));

  int name_index = distributions_.first_names().PickRandomIndex(
      2, &Stream(CustomerGeneratorColumn::kFirstName));
  row.first_name = distributions_.first_names().GetValueAtIndex(0, name_index);
  row.last_name = distributions_.last_names().PickRandomValue(
      0, 0, &Stream(CustomerGeneratorColumn::kLastName));
  int female_weight =
      distributions_.first_names().GetWeightForIndex(name_index, 1);
  int salutation_weight_index = female_weight == 0 ? 1 : 2;
  row.salutation = distributions_.salutations().PickRandomValue(
      0, salutation_weight_index,
      &Stream(CustomerGeneratorColumn::kSalutation));

  Date birthday = GenerateUniformRandomDate(
      kBirthMin, kBirthMax, &Stream(CustomerGeneratorColumn::kBirthDay));
  row.birth_day = birthday.day;
  row.birth_month = birthday.month;
  row.birth_year = birthday.year;

  row.email_address =
      GenerateRandomEmail(row.first_name, row.last_name,
                          &Stream(CustomerGeneratorColumn::kEmailAddress),
                          distributions_.top_domains());

  Date last_review = GenerateUniformRandomDate(
      one_year_ago_, today_, &Stream(CustomerGeneratorColumn::kLastReviewDate));
  row.last_review_date_sk = Date::ToJulianDays(last_review);

  Date first_sales = GenerateUniformRandomDate(
      ten_years_ago_, today_,
      &Stream(CustomerGeneratorColumn::kFirstSalesDateId));
  row.first_sales_date_sk = Date::ToJulianDays(first_sales);
  row.first_shipto_date_sk = row.first_sales_date_sk + 30;

  row.birth_country = distributions_.countries().PickRandomValue(
      0, 0, &Stream(CustomerGeneratorColumn::kBirthCountry));

  int threshold = GenerateUniformRandomInt(
      0, 9999, &Stream(CustomerGeneratorColumn::kNulls));
  int64_t bitmap = GenerateUniformRandomKey(
      1, kMaxInt, &Stream(CustomerGeneratorColumn::kNulls));
  if (threshold < kCustomerNullBasisPoints) {
    row.null_bitmap = bitmap & ~kCustomerNotNullBitMap;
  } else {
    row.null_bitmap = 0;
  }

  return row;
}

void CustomerRowGenerator::ConsumeRemainingSeedsForRow() {
  for (auto& stream : streams_) {
    while (stream.seeds_used() < stream.seeds_per_row()) {
      GenerateUniformRandomInt(1, 100, &stream);
    }
    stream.ResetSeedsUsed();
  }
}

RandomNumberStream& CustomerRowGenerator::Stream(
    CustomerGeneratorColumn column) {
  return streams_.at(static_cast<size_t>(column));
}

bool CustomerRowGenerator::IsNull(int64_t null_bitmap,
                                  CustomerGeneratorColumn column) {
  const auto& info = CustomerColumnInfos().at(static_cast<size_t>(column));
  int bit = info.global_column_number - kCustomerFirstColumn;
  return (null_bitmap & (1LL << bit)) != 0;
}

}  // namespace benchgen::tpcds::internal
