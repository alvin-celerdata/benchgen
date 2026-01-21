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

#include "utils/constants.h"

namespace benchgen::ssb::internal {

struct SeedState {
  DbgenTable table = DbgenTable::kNone;
  int64_t value = 0;
  int64_t usage = 0;
  int64_t boundary = 0;
};

class RandomState {
 public:
  RandomState();

  void Reset();
  void RowStart();
  void RowStop(DbgenTable table);

  int64_t RandomInt(int64_t low, int64_t high, int stream);
  double RandomDouble(double low, double high, int stream);
  double RandomExponential(double mean, int stream);
  int64_t PeekRandomInt(int64_t low, int64_t high, int stream) const;

  int64_t SeedValue(int stream) const;
  int64_t SeedBoundary(int stream) const;
  void AdvanceStream(int stream, int64_t count);

 private:
  static int NormalizeStream(int stream);
  static int64_t NextRand(int64_t seed);
  static void NthElement(int64_t count, int64_t* seed);

  std::array<SeedState, kMaxStream + 1> seeds_{};
};

void SkipPart(RandomState* rng, int64_t skip_count);
void SkipSupplier(RandomState* rng, int64_t skip_count);
void SkipCustomer(RandomState* rng, int64_t skip_count);
void SkipOrder(RandomState* rng, int64_t skip_count);
void SkipLine(RandomState* rng, int64_t skip_count, bool child);

}  // namespace benchgen::ssb::internal
