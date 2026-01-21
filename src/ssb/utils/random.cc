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

namespace benchgen::ssb::internal {
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
    case DbgenTable::kOrder:
      return DbgenTable::kLine;
    default:
      return DbgenTable::kNone;
  }
}

const std::array<SeedState, kMaxStream + 1> kInitialSeeds = {{
    {DbgenTable::kPart, 1, 0, 1},
    {DbgenTable::kPart, 46831694, 0, 1},
    {DbgenTable::kPart, 1841581359, 0, 1},
    {DbgenTable::kPart, 1193163244, 0, 1},
    {DbgenTable::kPart, 727633698, 0, 1},
    {DbgenTable::kNone, 933588178, 0, 1},
    {DbgenTable::kPart, 804159733, 0, kRngPerSentence * 3},
    {DbgenTable::kPartSupp, 1671059989, 0, 4},
    {DbgenTable::kPartSupp, 1051288424, 0, 4},
    {DbgenTable::kPartSupp, 1961692154, 0, 4 * kRngPerSentence * 20},
    {DbgenTable::kOrder, 1227283347, 0, 1},
    {DbgenTable::kOrder, 1171034773, 0, 1},
    {DbgenTable::kOrder, 276090261, 0, kRngPerSentence * 8},
    {DbgenTable::kOrder, 1066728069, 0, 1},
    {DbgenTable::kLine, 209208115, 0, kOLcntMax},
    {DbgenTable::kLine, 554590007, 0, kOLcntMax},
    {DbgenTable::kLine, 721958466, 0, kOLcntMax},
    {DbgenTable::kLine, 1371272478, 0, kOLcntMax},
    {DbgenTable::kLine, 675466456, 0, kOLcntMax},
    {DbgenTable::kLine, 1808217256, 0, kOLcntMax},
    {DbgenTable::kLine, 2095021727, 0, kOLcntMax},
    {DbgenTable::kLine, 1769349045, 0, kOLcntMax},
    {DbgenTable::kLine, 904914315, 0, kOLcntMax},
    {DbgenTable::kLine, 373135028, 0, kOLcntMax},
    {DbgenTable::kLine, 717419739, 0, kOLcntMax},
    {DbgenTable::kLine, 1095462486, 0, kOLcntMax * kRngPerSentence * 5},
    {DbgenTable::kCust, 881155353, 0, 9},
    {DbgenTable::kCust, 1489529863, 0, 1},
    {DbgenTable::kCust, 1521138112, 0, 3},
    {DbgenTable::kCust, 298370230, 0, 1},
    {DbgenTable::kCust, 1140279430, 0, 1},
    {DbgenTable::kCust, 1335826707, 0, kRngPerSentence * 12},
    {DbgenTable::kSupp, 706178559, 0, 9},
    {DbgenTable::kSupp, 110356601, 0, 1},
    {DbgenTable::kSupp, 884434366, 0, 3},
    {DbgenTable::kSupp, 962338209, 0, 1},
    {DbgenTable::kSupp, 1341315363, 0, kRngPerSentence * 11},
    {DbgenTable::kPart, 709314158, 0, kMaxColor},
    {DbgenTable::kOrder, 591449447, 0, 1},
    {DbgenTable::kLine, 431918286, 0, 1},
    {DbgenTable::kOrder, 851767375, 0, 1},
    {DbgenTable::kNation, 606179079, 0, kRngPerSentence * 16},
    {DbgenTable::kRegion, 1500869201, 0, kRngPerSentence * 16},
    {DbgenTable::kOrder, 1434868289, 0, 1},
    {DbgenTable::kSupp, 263032577, 0, 1},
    {DbgenTable::kSupp, 753643799, 0, 1},
    {DbgenTable::kSupp, 202794285, 0, 1},
    {DbgenTable::kSupp, 715851524, 0, 1},
}};

}  // namespace

RandomState::RandomState() { Reset(); }

void RandomState::Reset() { seeds_ = kInitialSeeds; }

void RandomState::RowStart() {
  for (auto& seed : seeds_) {
    seed.usage = 0;
  }
}

void RandomState::RowStop(DbgenTable table) {
  if (table == DbgenTable::kOrderLine) {
    table = DbgenTable::kOrder;
  } else if (table == DbgenTable::kPartPsupp) {
    table = DbgenTable::kPart;
  }
  DbgenTable child = ChildTable(table);
  for (auto& seed : seeds_) {
    if (seed.table == table || seed.table == child) {
      int64_t remaining = seed.boundary - seed.usage;
      if (remaining > 0) {
        NthElement(remaining, &seed.value);
      }
    }
  }
}

int64_t RandomState::RandomInt(int64_t low, int64_t high, int stream) {
  int index = NormalizeStream(stream);
  if (low > high) {
    std::swap(low, high);
  }
  double range = static_cast<double>(high - low + 1);
  seeds_[index].value = NextRand(seeds_[index].value);
  int64_t ntemp = static_cast<int64_t>(
      (static_cast<double>(seeds_[index].value) / kModulusDouble) * range);
  seeds_[index].usage += 1;
  return low + ntemp;
}

double RandomState::RandomDouble(double low, double high, int stream) {
  int index = NormalizeStream(stream);
  if (low == high) {
    return low;
  }
  if (low > high) {
    std::swap(low, high);
  }
  seeds_[index].value = NextRand(seeds_[index].value);
  double value = (static_cast<double>(seeds_[index].value) / kModulusDouble) *
                 (high - low);
  seeds_[index].usage += 1;
  return low + value;
}

double RandomState::RandomExponential(double mean, int stream) {
  if (mean <= 0.0) {
    return 0.0;
  }
  int index = NormalizeStream(stream);
  seeds_[index].value = NextRand(seeds_[index].value);
  double value = static_cast<double>(seeds_[index].value) / kModulusDouble;
  seeds_[index].usage += 1;
  return (-mean * std::log(1.0 - value));
}

int64_t RandomState::PeekRandomInt(int64_t low, int64_t high,
                                   int stream) const {
  int index = NormalizeStream(stream);
  if (low > high) {
    std::swap(low, high);
  }
  double range = static_cast<double>(high - low + 1);
  int64_t next = NextRand(seeds_[index].value);
  int64_t ntemp = static_cast<int64_t>(
      (static_cast<double>(next) / kModulusDouble) * range);
  return low + ntemp;
}

int64_t RandomState::SeedValue(int stream) const {
  int index = NormalizeStream(stream);
  return seeds_[index].value;
}

int64_t RandomState::SeedBoundary(int stream) const {
  int index = NormalizeStream(stream);
  return seeds_[index].boundary;
}

void RandomState::AdvanceStream(int stream, int64_t count) {
  int index = NormalizeStream(stream);
  if (count <= 0) {
    return;
  }
  NthElement(count, &seeds_[index].value);
}

int RandomState::NormalizeStream(int stream) {
  if (stream < 0 || stream > kMaxStream) {
    return 0;
  }
  return stream;
}

int64_t RandomState::NextRand(int64_t seed) {
  int64_t div = seed / kQuotient;
  int64_t mod = seed - kQuotient * div;
  int64_t next = kMultiplier * mod - div * kRemainder;
  if (next < 0) {
    next += kModulus;
  }
  return next;
}

void RandomState::NthElement(int64_t count, int64_t* seed) {
  if (!seed || count <= 0) {
    return;
  }
  int64_t mult = kMultiplier;
  int64_t value = *seed;
  while (count > 0) {
    if (count % 2 != 0) {
      value = (mult * value) % kModulus;
    }
    count /= 2;
    mult = (mult * mult) % kModulus;
  }
  *seed = value;
}

void SkipPart(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  for (int stream = kPMfgSd; stream <= kPCntrSd; ++stream) {
    rng->AdvanceStream(stream, skip_count);
  }
  rng->AdvanceStream(kPCatSd, skip_count);
  rng->AdvanceStream(kPCmntSd, rng->SeedBoundary(kPCmntSd) * skip_count);
  rng->AdvanceStream(kPNameSd, static_cast<int64_t>(kMaxColor) * skip_count);
}

void SkipSupplier(RandomState* rng, int64_t skip_count) {
  if (!rng || skip_count <= 0) {
    return;
  }
  rng->AdvanceStream(kSNtrgSd, skip_count);
  rng->AdvanceStream(kCPhneSd, 3 * skip_count);
  rng->AdvanceStream(kSAbalSd, skip_count);
  rng->AdvanceStream(kSAddrSd, rng->SeedBoundary(kSAddrSd) * skip_count);
  rng->AdvanceStream(kSCmntSd, rng->SeedBoundary(kSCmntSd) * skip_count);
  rng->AdvanceStream(kBbbCmntSd, skip_count);
  rng->AdvanceStream(kBbbJnkSd, skip_count);
  rng->AdvanceStream(kBbbOffsetSd, skip_count);
  rng->AdvanceStream(kBbbTypeSd, skip_count);
  // GenerateCity uses stream 98 (normalized to stream 0), so advance it too.
  rng->AdvanceStream(98, skip_count);
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
  // GenerateCity uses stream 98 (normalized to stream 0), so advance it too.
  rng->AdvanceStream(98, skip_count);
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
  for (int64_t j = 0; j < kOLcntMax; ++j) {
    for (int stream = kLQtySd; stream <= kLRflgSd; ++stream) {
      rng->AdvanceStream(stream, skip_count);
    }
  }
  rng->AdvanceStream(kLCmntSd, rng->SeedBoundary(kLCmntSd) * skip_count);
  if (child) {
    rng->AdvanceStream(kOOdateSd, skip_count);
    rng->AdvanceStream(kOLcntSd, skip_count);
  }
}

}  // namespace benchgen::ssb::internal
