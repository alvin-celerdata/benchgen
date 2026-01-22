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

namespace benchgen::tpch::internal {

class RandomStream {
 public:
  RandomStream() = default;
  RandomStream(DbgenTable table, int64_t seed, int64_t boundary);

  void Reset(DbgenTable table, int64_t seed, int64_t boundary);
  void ResetUsage();
  void AdvanceToBoundary();

  int64_t NextInt(int64_t low, int64_t high);
  double NextDouble(double low, double high);
  double NextExponential(double mean);
  void Advance(int64_t count);

  DbgenTable table() const { return table_; }
  int64_t value() const { return value_; }
  int64_t usage() const { return usage_; }
  int64_t boundary() const { return boundary_; }

 private:
  static int64_t NextRand(int64_t seed);
  static int64_t NthElement(int64_t count, int64_t seed);

  DbgenTable table_ = DbgenTable::kNone;
  int64_t value_ = 0;
  int64_t usage_ = 0;
  int64_t boundary_ = 0;
};

class RandomState {
 public:
  RandomState();

  void Reset();
  void RowStart();
  void RowStop(DbgenTable table);

  int64_t RandomInt(int64_t low, int64_t high, int stream);
  int64_t PeekRandomInt(int64_t low, int64_t high, int stream) const;
  double RandomDouble(double low, double high, int stream);
  double RandomExponential(double mean, int stream);

  int64_t SeedValue(int stream) const;
  int64_t SeedBoundary(int stream) const;
  void AdvanceStream(int stream, int64_t count);

 private:
  static int NormalizeStream(int stream);

  std::array<RandomStream, kMaxStream + 1> streams_{};
};

void SkipPart(RandomState* rng, int64_t skip_count);
void SkipPartSupp(RandomState* rng, int64_t skip_count);
void SkipSupplier(RandomState* rng, int64_t skip_count);
void SkipCustomer(RandomState* rng, int64_t skip_count);
void SkipOrder(RandomState* rng, int64_t skip_count);
void SkipLine(RandomState* rng, int64_t skip_count, bool child);

}  // namespace benchgen::tpch::internal
