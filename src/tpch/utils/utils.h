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
#include <string>
#include <vector>

#include "distribution/distribution.h"
#include "utils/random.h"

namespace benchgen::tpch::internal {

int RandomString(int min_len, int max_len, int stream, RandomState* rng,
                 std::string* out);
int VariableString(int avg_len, int stream, RandomState* rng, std::string* out);

int PickString(const Distribution& dist, int stream, RandomState* rng,
               std::string* out);
void AggString(const Distribution& dist, int count, int stream,
               RandomState* rng, std::string* out);

void GeneratePhone(int64_t nation_index, int stream, RandomState* rng,
                   std::string* out);

int64_t RetailPrice(int64_t partkey);
int64_t PartSuppBridge(int64_t partkey, int64_t supp_index,
                       int64_t supplier_count);
int64_t MakeSparseKey(int64_t index, int64_t seq);

int64_t OrderDateMax();
int64_t JulianDate(int64_t date);
void BuildAscDate(std::vector<std::string>* out);

std::string FormatTagNumber(const char* tag, int width, int64_t number);

}  // namespace benchgen::tpch::internal
