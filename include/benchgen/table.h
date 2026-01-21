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

#include <string_view>

namespace benchgen {

namespace tpch {

enum class TableId {
  kPart = 0,
  kPartSupp,
  kSupplier,
  kCustomer,
  kOrders,
  kLineItem,
  kNation,
  kRegion,
  kTableCount
};

std::string_view TableIdToString(TableId table);
bool TableIdFromString(std::string_view name, TableId* out);

}  // namespace tpch

namespace tpcds {

enum class TableId {
  kCallCenter = 0,
  kCatalogPage,
  kCatalogReturns,
  kCatalogSales,
  kCustomer,
  kCustomerAddress,
  kCustomerDemographics,
  kDateDim,
  kHouseholdDemographics,
  kIncomeBand,
  kInventory,
  kItem,
  kPromotion,
  kReason,
  kShipMode,
  kStore,
  kStoreReturns,
  kStoreSales,
  kTimeDim,
  kWarehouse,
  kWebPage,
  kWebReturns,
  kWebSales,
  kWebSite,
  kTableCount
};

std::string_view TableIdToString(TableId table);
bool TableIdFromString(std::string_view name, TableId* out);

}  // namespace tpcds

namespace ssb {

enum class TableId {
  kCustomer = 0,
  kPart,
  kSupplier,
  kDate,
  kLineorder,
  kTableCount
};

std::string_view TableIdToString(TableId table);
bool TableIdFromString(std::string_view name, TableId* out);

}  // namespace ssb

}  // namespace benchgen
