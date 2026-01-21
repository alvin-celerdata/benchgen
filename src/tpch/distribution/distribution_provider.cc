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

#include "distribution/distribution_source.h"

#include "distribution/embedded_distribution.h"

namespace benchgen::tpch::internal {
namespace {

constexpr const char* kEmbeddedDistributionLabel = "<embedded>";

std::string DescribeSource(const DistributionSource& source) {
  if (source.kind == DistributionSourceKind::kEmbedded) {
    return kEmbeddedDistributionLabel;
  }
  return kEmbeddedDistributionLabel;
}

arrow::Status LoadStore(const DistributionSource& source,
                        DistributionStore* store) {
  if (!store) {
    return arrow::Status::Invalid("distribution store must be provided");
  }
  (void)source;
  return store->LoadFromBuffer(EmbeddedDistributionData(),
                               EmbeddedDistributionSize());
}

bool SourcesMatch(const DistributionSource& loaded,
                  const DistributionSource& requested) {
  if (loaded.kind != requested.kind) {
    return false;
  }
  if (loaded.kind == DistributionSourceKind::kFilesystem) {
    return loaded.path == requested.path;
  }
  return true;
}

}  // namespace

arrow::Status DistributionProvider::Init() {
  DistributionSource source = ResolveDistributionSource();
  if (initialized_) {
    if (!SourcesMatch(source_, source)) {
      return arrow::Status::Invalid("distribution path already initialized to ",
                                    DescribeSource(source_), "; requested ",
                                    DescribeSource(source));
    }
    return arrow::Status::OK();
  }

  auto status = LoadStore(source, &store_);
  if (!status.ok()) {
    return status;
  }

  source_ = source;
  initialized_ = true;
  return arrow::Status::OK();
}

}  // namespace benchgen::tpch::internal
