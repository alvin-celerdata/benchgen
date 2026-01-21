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

#include "ssb_types.h"
#include "distribution/distribution.h"
#include "utils/random.h"

namespace benchgen::ssb::internal {

struct DbgenDistributions;

int RandomString(int min, int max, int stream, RandomState* rng,
                 std::string* dest);
int VariableString(int avg, int stream, RandomState* rng,
                   std::string* dest);

int PickString(const Distribution& dist, int stream, RandomState* rng,
               std::string* target);
void AggString(const Distribution& dist, int count, int stream,
               RandomState* rng, std::string* dest);

void GeneratePhone(int64_t ind, std::string* target, int stream,
                   RandomState* rng);
void GenerateCategory(std::string* target, int stream, RandomState* rng);
int GenerateCity(std::string* city_name, const std::string& nation_name,
                 RandomState* rng);
int GenerateColor(std::string* source, std::string* dest);

int GenerateText(int min, int max, const DbgenDistributions& dists, int stream,
                 RandomState* rng, std::string* dest);

int64_t RetailPrice(int64_t partkey);

void BuildAscDate(std::vector<std::string>* out);
void GenerateDateRow(int64_t index, date_t* out);

}  // namespace benchgen::ssb::internal
