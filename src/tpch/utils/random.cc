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

#include "utils/random.h"

#include <algorithm>
#include <cmath>

namespace benchgen::tpch::internal {
namespace {

constexpr int64_t kMultiplier = 16807;
constexpr int64_t kModulus = 2147483647;
constexpr int64_t kQuotient = 127773;
constexpr int64_t kRemainder = 2836;
constexpr double kModulusDouble = 2147483647.0;

DbgenTable ChildTable(DbgenTable table) {
  switch (table) {
    case DbgenTable::kPart:
      return DbgenTable::kPartSupp;
    case DbgenTable::kOrders:
      return DbgenTable::kLineItem;
    default:
      return DbgenTable::kNone;
  }
}

struct StreamSeed {
  DbgenTable table;
  int64_t seed;
  int64_t boundary;
};

const std::array<StreamSeed, kMaxStream + 1> kInitialSeeds = {{
    {DbgenTable::kPart, 1, 1},
    {DbgenTable::kPart, 46831694, 1},
    {DbgenTable::kPart, 1841581359, 1},
    {DbgenTable::kPart, 1193163244, 1},
    {DbgenTable::kPart, 727633698, 1},
    {DbgenTable::kNone, 933588178, 1},
    {DbgenTable::kPart, 804159733, 2},
    {DbgenTable::kPartSupp, 1671059989, 4},
    {DbgenTable::kPartSupp, 1051288424, 4},
    {DbgenTable::kPartSupp, 1961692154, 2},
    {DbgenTable::kOrders, 1227283347, 1},
    {DbgenTable::kOrders, 1171034773, 1},
    {DbgenTable::kOrders, 276090261, 2},
    {DbgenTable::kOrders, 1066728069, 1},
    {DbgenTable::kLineItem, 209208115, kOLcntMax},
    {DbgenTable::kLineItem, 554590007, kOLcntMax},
    {DbgenTable::kLineItem, 721958466, kOLcntMax},
    {DbgenTable::kLineItem, 1371272478, kOLcntMax},
    {DbgenTable::kLineItem, 675466456, kOLcntMax},
    {DbgenTable::kLineItem, 1808217256, kOLcntMax},
    {DbgenTable::kLineItem, 2095021727, kOLcntMax},
    {DbgenTable::kLineItem, 1769349045, kOLcntMax},
    {DbgenTable::kLineItem, 904914315, kOLcntMax},
    {DbgenTable::kLineItem, 373135028, kOLcntMax},
    {DbgenTable::kLineItem, 717419739, kOLcntMax},
    {DbgenTable::kLineItem, 1095462486, kOLcntMax * 2},
    {DbgenTable::kCustomer, 881155353, 9},
    {DbgenTable::kCustomer, 1489529863, 1},
    {DbgenTable::kCustomer, 1521138112, 3},
    {DbgenTable::kCustomer, 298370230, 1},
    {DbgenTable::kCustomer, 1140279430, 1},
    {DbgenTable::kCustomer, 1335826707, 2},
    {DbgenTable::kSupplier, 706178559, 9},
    {DbgenTable::kSupplier, 110356601, 1},
    {DbgenTable::kSupplier, 884434366, 3},
    {DbgenTable::kSupplier, 962338209, 1},
    {DbgenTable::kSupplier, 1341315363, 2},
    {DbgenTable::kPart, 709314158, kMaxColor},
    {DbgenTable::kOrders, 591449447, 1},
    {DbgenTable::kLineItem, 431918286, 1},
    {DbgenTable::kOrders, 851767375, 1},
    {DbgenTable::kNation, 606179079, 2},
    {DbgenTable::kRegion, 1500869201, 2},
    {DbgenTable::kOrders, 1434868289, 1},
    {DbgenTable::kSupplier, 263032577, 1},
    {DbgenTable::kSupplier, 753643799, 1},
    {DbgenTable::kSupplier, 202794285, 1},
    {DbgenTable::kSupplier, 715851524, 1},
}};

}  // namespace

RandomStream::RandomStream(DbgenTable table, int64_t seed, int64_t boundary) {
  Reset(table, seed, boundary);
}

void RandomStream::Reset(DbgenTable table, int64_t seed, int64_t boundary) {
  table_ = table;
  value_ = seed;
  boundary_ = boundary;
  usage_ = 0;
}

void RandomStream::ResetUsage() { usage_ = 0; }

void RandomStream::AdvanceToBoundary() {
  int64_t remaining = boundary_ - usage_;
  if (remaining > 0) {
    value_ = NthElement(remaining, value_);
  }
}

int64_t RandomStream::NextInt(int64_t low, int64_t high) {
  if (low > high) {
    std::swap(low, high);
  }
  double range = static_cast<double>(high - low + 1);
  if (low == 0 && high == kMaxLong) {
    // Match dbgen's int32_t overflow behavior for MAX_LONG ranges.
    range = -static_cast<double>(static_cast<uint32_t>(high) + 1u);
  }
  value_ = NextRand(value_);
  int64_t ntemp = static_cast<int64_t>(
      (static_cast<double>(value_) / kModulusDouble) * range);
  usage_ += 1;
  return low + ntemp;
}

double RandomStream::NextDouble(double low, double high) {
  if (low == high) {
    return low;
  }
  if (low > high) {
    std::swap(low, high);
  }
  value_ = NextRand(value_);
  double value = (static_cast<double>(value_) / kModulusDouble) * (high - low);
  usage_ += 1;
  return low + value;
}

double RandomStream::NextExponential(double mean) {
  if (mean <= 0.0) {
    return 0.0;
  }
  value_ = NextRand(value_);
  double value = static_cast<double>(value_) / kModulusDouble;
  usage_ += 1;
  return (-mean * std::log(1.0 - value));
}

void RandomStream::Advance(int64_t count) {
  if (count <= 0) {
    return;
  }
  value_ = NthElement(count, value_);
}

int64_t RandomStream::NextRand(int64_t seed) {
  int64_t div = seed / kQuotient;
  int64_t mod = seed - kQuotient * div;
  int64_t next = kMultiplier * mod - div * kRemainder;
  if (next < 0) {
    next += kModulus;
  }
  return next;
}

int64_t RandomStream::NthElement(int64_t count, int64_t seed) {
  if (count <= 0) {
    return seed;
  }
  int64_t mult = kMultiplier;
  int64_t value = seed;
  while (count > 0) {
    if (count % 2 != 0) {
      value = (mult * value) % kModulus;
    }
    count /= 2;
    mult = (mult * mult) % kModulus;
  }
  return value;
}

RandomState::RandomState() { Reset(); }

void RandomState::Reset() {
  for (std::size_t i = 0; i < streams_.size(); ++i) {
    const auto& seed = kInitialSeeds[i];
    streams_[i].Reset(seed.table, seed.seed, seed.boundary);
  }
}

void RandomState::RowStart() {
  for (auto& stream : streams_) {
    stream.ResetUsage();
  }
}

void RandomState::RowStop(DbgenTable table) {
  if (table == DbgenTable::kOrderLine) {
    table = DbgenTable::kOrders;
  } else if (table == DbgenTable::kPartPsupp) {
    table = DbgenTable::kPart;
  }
  DbgenTable child = ChildTable(table);
  for (auto& stream : streams_) {
    if (stream.table() == table || stream.table() == child) {
      stream.AdvanceToBoundary();
    }
  }
}

int64_t RandomState::RandomInt(int64_t low, int64_t high, int stream) {
  int index = NormalizeStream(stream);
  return streams_[index].NextInt(low, high);
}

double RandomState::RandomDouble(double low, double high, int stream) {
  int index = NormalizeStream(stream);
  return streams_[index].NextDouble(low, high);
}

double RandomState::RandomExponential(double mean, int stream) {
  int index = NormalizeStream(stream);
  return streams_[index].NextExponential(mean);
}

int64_t RandomState::SeedValue(int stream) const {
  int index = NormalizeStream(stream);
  return streams_[index].value();
}

int64_t RandomState::SeedBoundary(int stream) const {
  int index = NormalizeStream(stream);
  return streams_[index].boundary();
}

void RandomState::AdvanceStream(int stream, int64_t count) {
  int index = NormalizeStream(stream);
  streams_[index].Advance(count);
}

int RandomState::NormalizeStream(int stream) {
  if (stream < 0 || stream > kMaxStream) {
    return 0;
  }
  return stream;
}

void SkipPart(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  for (int stream = kPMfgSd; stream <= kPCntrSd; ++stream) {
    rng->AdvanceStream(stream, skip_count);
  }
  rng->AdvanceStream(kPCmntSd, rng->SeedBoundary(kPCmntSd) * skip_count);
  rng->AdvanceStream(kPNameSd, static_cast<int64_t>(kMaxColor) * skip_count);
}

void SkipPartSupp(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  for (int64_t j = 0; j < kSuppPerPart; ++j) {
    rng->AdvanceStream(kPsQtySd, skip_count);
    rng->AdvanceStream(kPsScstSd, skip_count);
    rng->AdvanceStream(kPsCmntSd, rng->SeedBoundary(kPsCmntSd) * skip_count);
  }
}

void SkipSupplier(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  rng->AdvanceStream(kSNtrgSd, skip_count);
  rng->AdvanceStream(kSPhneSd, 3 * skip_count);
  rng->AdvanceStream(kSAbalSd, skip_count);
  rng->AdvanceStream(kSAddrSd, rng->SeedBoundary(kSAddrSd) * skip_count);
  rng->AdvanceStream(kSCmntSd, rng->SeedBoundary(kSCmntSd) * skip_count);
  rng->AdvanceStream(kBbbCmntSd, skip_count);
  rng->AdvanceStream(kBbbJnkSd, skip_count);
  rng->AdvanceStream(kBbbOffsetSd, skip_count);
  rng->AdvanceStream(kBbbTypeSd, skip_count);
}

void SkipCustomer(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  rng->AdvanceStream(kCAddrSd, rng->SeedBoundary(kCAddrSd) * skip_count);
  rng->AdvanceStream(kCCmntSd, rng->SeedBoundary(kCCmntSd) * skip_count);
  rng->AdvanceStream(kCNtrgSd, skip_count);
  rng->AdvanceStream(kCPhneSd, 3 * skip_count);
  rng->AdvanceStream(kCAbalSd, skip_count);
  rng->AdvanceStream(kCMsegSd, skip_count);
}

void SkipOrder(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  rng->AdvanceStream(kOLcntSd, skip_count);
  rng->AdvanceStream(kOCkeySd, skip_count);
  rng->AdvanceStream(kOCmntSd, rng->SeedBoundary(kOCmntSd) * skip_count);
  rng->AdvanceStream(kOSuppSd, skip_count);
  rng->AdvanceStream(kOClrkSd, skip_count);
  rng->AdvanceStream(kOPrioSd, skip_count);
  rng->AdvanceStream(kOOdateSd, skip_count);
}

void SkipLine(RandomState* rng, int64_t skip_count, bool child) {
  if (!rng || skip_count <= 0) {
    return;
  }
  int64_t comment_per_line = rng->SeedBoundary(kLCmntSd) / kOLcntMax;
  if (comment_per_line <= 0) {
    comment_per_line = 1;
  }
  for (int64_t j = 0; j < kOLcntMax; ++j) {
    for (int stream = kLQtySd; stream <= kLRflgSd; ++stream) {
      rng->AdvanceStream(stream, skip_count);
    }
    rng->AdvanceStream(kLCmntSd, comment_per_line * skip_count);
  }
  if (child) {
    rng->AdvanceStream(kOOdateSd, skip_count);
    rng->AdvanceStream(kOLcntSd, skip_count);
  }
}

}  // namespace benchgen::tpch::internal
