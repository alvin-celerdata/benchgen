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

#include "distribution/distribution_provider.h"

#include "distribution/dst_distribution_store.h"

namespace benchgen::tpcds::internal {

DistributionProvider::DistributionProvider() {
  DstDistributionStore store;
  first_names_ =
      StringValuesDistribution::FromDstDistribution(store.Get("first_names"));
  last_names_ =
      StringValuesDistribution::FromDstDistribution(store.Get("last_names"));
  salutations_ =
      StringValuesDistribution::FromDstDistribution(store.Get("salutations"));
  countries_ =
      StringValuesDistribution::FromDstDistribution(store.Get("countries"));
  top_domains_ =
      StringValuesDistribution::FromDstDistribution(store.Get("top_domains"));
}

}  // namespace benchgen::tpcds::internal
