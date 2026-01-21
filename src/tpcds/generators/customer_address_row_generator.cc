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

#include "generators/customer_address_row_generator.h"

#include <cstdio>

#include "utils/columns.h"
#include "utils/null_utils.h"
#include "utils/random_utils.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {
namespace {

std::string FormatZip(int zip) {
  char buffer[6] = {};
  std::snprintf(buffer, sizeof(buffer), "%05d", zip);
  return std::string(buffer);
}

}  // namespace

CustomerAddressRowGenerator::CustomerAddressRowGenerator(
    double scale)
    : scaling_(scale),
      distribution_store_(),
      streams_(ColumnIds()) {
  location_type_ = &distribution_store_.Get("location_type");
}

void CustomerAddressRowGenerator::SkipRows(int64_t start_row) {
  streams_.SkipRows(start_row);
}

CustomerAddressRowData CustomerAddressRowGenerator::GenerateRow(
    int64_t row_number) {
  CustomerAddressRowData row;
  row.address_sk = row_number;
  row.address_id = MakeBusinessKey(static_cast<uint64_t>(row_number));

  row.null_bitmap =
      GenerateNullBitmap(CUSTOMER_ADDRESS, &streams_.Stream(CA_NULLS));

  Address address = GenerateAddress(CUSTOMER_ADDRESS, &distribution_store_,
                                    &streams_.Stream(CA_ADDRESS), scaling_);

  row.street_num = address.street_num;
  row.street_name = address.street_name1 + " " + address.street_name2;
  row.street_type = address.street_type;
  row.suite_num = address.suite_num;
  row.city = address.city;
  row.county = address.county;
  row.state = address.state;
  row.zip = FormatZip(address.zip);
  row.country = address.country;
  row.gmt_offset = address.gmt_offset;

  int location_index =
      location_type_->PickIndex(1, &streams_.Stream(CA_LOCATION_TYPE));
  row.location_type = location_type_->GetString(location_index, 1);

  return row;
}

void CustomerAddressRowGenerator::ConsumeRemainingSeedsForRow() {
  streams_.ConsumeRemainingSeedsForRow();
}

std::vector<int> CustomerAddressRowGenerator::ColumnIds() {
  std::vector<int> ids;
  ids.reserve(
      static_cast<size_t>(CUSTOMER_ADDRESS_END - CUSTOMER_ADDRESS_START + 1));
  for (int column = CUSTOMER_ADDRESS_START; column <= CUSTOMER_ADDRESS_END;
       ++column) {
    ids.push_back(column);
  }
  return ids;
}

}  // namespace benchgen::tpcds::internal
