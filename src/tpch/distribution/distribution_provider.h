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

#include <arrow/status.h>

#include <string>

#include "distribution/distribution.h"
#include "distribution/distribution_source.h"

namespace benchgen::tpch::internal {

class DistributionProvider {
 public:
  DistributionProvider() = default;

  arrow::Status Init();

  const DistributionStore& store() const { return store_; }
  const DistributionSource& source() const { return source_; }
  bool initialized() const { return initialized_; }

 private:
  bool initialized_ = false;
  DistributionSource source_{DistributionSourceKind::kEmbedded, ""};
  DistributionStore store_;
};

}  // namespace benchgen::tpch::internal
