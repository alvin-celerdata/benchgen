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

#include "distribution/dst_distribution_store.h"
#include "distribution/scaling.h"
#include "utils/random_number_stream.h"

namespace benchgen::tpcds::internal {

int64_t MakeJoin(int from_column, int to_table, int64_t join_count,
                 RandomNumberStream* stream, const Scaling& scaling,
                 DstDistributionStore* store);

int64_t DateJoin(int from_table, int from_column, int64_t join_count, int year,
                 RandomNumberStream* stream, const Scaling& scaling,
                 const DstDistribution& calendar);

int64_t TimeJoin(int from_table, RandomNumberStream* stream,
                 const DstDistribution& hours);

int64_t CatalogPageJoin(int from_table, int from_column, int64_t julian_date,
                        RandomNumberStream* stream, const Scaling& scaling,
                        DstDistributionStore* store);

int64_t WebJoin(int column_id, int64_t join_key, RandomNumberStream* stream,
                const Scaling& scaling);

int64_t GetCatalogNumberFromPage(int64_t page_number, const Scaling& scaling);

}  // namespace benchgen::tpcds::internal
