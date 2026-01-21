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

#include "distribution/dst_distribution.h"
#include "distribution/scaling.h"

namespace benchgen::tpcds::internal {

int64_t DateScaling(int table_number, int64_t julian_date,
                    const Scaling& scaling, const DstDistribution& calendar);

int64_t SkipDays(int table_number, int64_t* remainder, const Scaling& scaling,
                 const DstDistribution& calendar);

}  // namespace benchgen::tpcds::internal
