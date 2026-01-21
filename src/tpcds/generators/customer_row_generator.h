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

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "distribution/distribution_provider.h"
#include "distribution/scaling.h"
#include "utils/date.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

enum class CustomerGeneratorColumn {
  kCustomerSk = 0,
  kCustomerId,
  kCurrentCdemoSk,
  kCurrentHdemoSk,
  kCurrentAddrSk,
  kFirstShiptoDateId,
  kFirstSalesDateId,
  kSalutation,
  kFirstName,
  kLastName,
  kPreferredCustFlag,
  kBirthDay,
  kBirthMonth,
  kBirthYear,
  kBirthCountry,
  kLogin,
  kEmailAddress,
  kLastReviewDate,
  kNulls,
  kCount
};

struct CustomerColumnInfo {
  CustomerGeneratorColumn column;
  int global_column_number;
  int seeds_per_row;
};

constexpr int kCustomerFirstColumn = 114;
constexpr int kCustomerNullBasisPoints = 700;
constexpr int64_t kCustomerNotNullBitMap = 0x13;
constexpr int kPreferredPct = 50;

const std::array<CustomerColumnInfo,
                 static_cast<size_t>(CustomerGeneratorColumn::kCount)>&
CustomerColumnInfos();

struct CustomerRowData {
  int64_t customer_sk = 0;
  std::string customer_id;
  int64_t current_cdemo_sk = 0;
  int64_t current_hdemo_sk = 0;
  int64_t current_addr_sk = 0;
  int32_t first_shipto_date_sk = 0;
  int32_t first_sales_date_sk = 0;
  std::string salutation;
  std::string first_name;
  std::string last_name;
  bool preferred_cust_flag = false;
  int32_t birth_day = 0;
  int32_t birth_month = 0;
  int32_t birth_year = 0;
  std::string birth_country;
  std::string email_address;
  int32_t last_review_date_sk = 0;
  int64_t null_bitmap = 0;
};

class CustomerRowGenerator {
 public:
  CustomerRowGenerator(double scale);

  void SkipRows(int64_t start_row);
  CustomerRowData GenerateRow(int64_t row_number);
  void ConsumeRemainingSeedsForRow();
  static bool IsNull(int64_t null_bitmap, CustomerGeneratorColumn column);

 private:
  RandomNumberStream& Stream(CustomerGeneratorColumn column);

  Scaling scaling_;
  DistributionProvider distributions_;
  std::vector<RandomNumberStream> streams_;
  Date today_{};
  Date one_year_ago_{};
  Date ten_years_ago_{};
};

}  // namespace benchgen::tpcds::internal
