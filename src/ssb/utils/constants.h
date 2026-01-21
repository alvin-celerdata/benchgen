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

namespace benchgen::ssb::internal {

enum class DbgenTable : int {
  kNone = -1,
  kPart = 0,
  kPartSupp = 1,
  kSupp = 2,
  kCust = 3,
  kOrder = 4,
  kLine = 5,
  kOrderLine = 6,
  kPartPsupp = 7,
  kNation = 8,
  kRegion = 9,
  kUpdate = 10,
};

constexpr DbgenTable kDateTable = DbgenTable::kOrder;

constexpr int kMaxStream = 47;
constexpr int64_t kMaxLong = 0x7fffffff;
constexpr int kMaxGrammarLen = 12;
constexpr int kMaxSentenceLen = 256;
constexpr int kMaxAggLen = 15;
constexpr int kMaxColor = 92;
constexpr int kRngPerSentence = 27;

constexpr double kVStrLow = 0.4;
constexpr double kVStrHigh = 1.6;

constexpr int kPNameLen = 22;
constexpr int kPMfgLen = 6;
constexpr int kPColorLen = 3;
constexpr int kPColorMax = 11;
constexpr int kPTypeMax = 25;
constexpr int kPCatLen = 7;
constexpr int kPBrndLen = 10;
constexpr int kPTypeLen = 12;
constexpr int kPCntrLen = 10;
constexpr int kPCommentLen = 14;
constexpr int kPCommentMax = 23;

constexpr int kSNameLen = 25;
constexpr int kSAddrLen = 15;
constexpr int kSAddrMax = 25;
constexpr int kSNationNameLen = 15;
constexpr int kSRegionNameLen = 12;
constexpr int kSCommentLen = 63;
constexpr int kSCommentMax = 101;

constexpr int kCNameLen = 25;
constexpr int kCAddrLen = 15;
constexpr int kCAddrMax = 25;
constexpr int kCMsegLen = 10;
constexpr int kCCommentLen = 73;
constexpr int kCCommentMax = 117;
constexpr int kCNationNameLen = 15;
constexpr int kCRegionNameLen = 12;

constexpr int kOPrioLen = 15;
constexpr int kOClrkLen = 15;
constexpr int kOCommentLen = 49;
constexpr int kOCommentMax = 79;
constexpr int kOShipModeLen = 10;

constexpr int kLCmntLen = 27;
constexpr int kLCmntMax = 44;
constexpr int kLInstLen = 25;
constexpr int kLSmodeLen = 10;

constexpr int kTAlphaLen = 10;
constexpr int kDateLen = 13;
constexpr int kNationLen = 25;
constexpr int kRegionLen = 25;
constexpr int kPhoneLen = 15;

constexpr int kDDateLen = 18;
constexpr int kDDayweekLen = 9;
constexpr int kDYearmonthLen = 7;
constexpr int kDSeasonLen = 12;
constexpr int kDMonthLen = 9;

constexpr int kCityFix = 10;
constexpr int kNumSeasons = 5;
constexpr int kNumHolidays = 10;

constexpr int64_t kStartDate = 92001;
constexpr int64_t kCurrentDate = 95168;
constexpr int64_t kEndDate = 98365;
constexpr int64_t kTotalDate = 2557;
constexpr int64_t kDStartDate = 694245661;
constexpr int64_t kPennies = 100;

constexpr int kSparseBits = 2;
constexpr int kSparseKeep = 3;

constexpr int64_t kPNameScl = 3;
constexpr int64_t kPartBase = 200000;
constexpr int64_t kSupplierBase = 2000;
constexpr int64_t kCustomerBase = 30000;
constexpr int64_t kPMfgMin = 1;
constexpr int64_t kPMfgMax = 5;
constexpr int64_t kPCatMin = 1;
constexpr int64_t kPCatMax = 5;
constexpr int64_t kPBrndMin = 1;
constexpr int64_t kPBrndMax = 40;
constexpr int64_t kPSizeMin = 1;
constexpr int64_t kPSizeMax = 50;

constexpr int64_t kSAbalMin = -99999;
constexpr int64_t kSAbalMax = 999999;
constexpr int64_t kCAbalMin = -99999;
constexpr int64_t kCAbalMax = 999999;

constexpr int64_t kCMsegMin = 1;
constexpr int64_t kCMsegMax = 5;

constexpr int64_t kOSize = 109;
constexpr int64_t kOCkeyMin = 1;
constexpr int64_t kOClrkScl = 1000;
constexpr int64_t kOLcntMin = 1;
constexpr int64_t kOLcntMax = 7;

constexpr int64_t kLQtyMin = 1;
constexpr int64_t kLQtyMax = 50;
constexpr int64_t kLTaxMin = 0;
constexpr int64_t kLTaxMax = 8;
constexpr int64_t kLDcntMin = 0;
constexpr int64_t kLDcntMax = 10;
constexpr int64_t kLPkeyMin = 1;
constexpr int64_t kLSkeyMin = 1;
constexpr int64_t kLSdteMin = 1;
constexpr int64_t kLSdteMax = 121;
constexpr int64_t kLCdteMin = 30;
constexpr int64_t kLCdteMax = 90;
constexpr int64_t kLRdteMin = 1;
constexpr int64_t kLRdteMax = 30;

constexpr int64_t kOrdersPerCustomer = 10;
constexpr int64_t kCustomerMortality = 3;
constexpr int64_t kNationsMax = 90;

inline constexpr const char kCNameTag[] = "Customer#";
inline constexpr const char kCNameFmt[] = "%s%09d";
inline constexpr const char kSNameTag[] = "Supplier#";
inline constexpr const char kSNameFmt[] = "%s%09ld";
inline constexpr const char kOClrkTag[] = "Clerk#";
inline constexpr const char kOClrkFmt[] = "%s%09d";

constexpr int kPMfgSd = 0;
constexpr int kPBrndSd = 1;
constexpr int kPTypeSd = 2;
constexpr int kPSizeSd = 3;
constexpr int kPCntrSd = 4;
constexpr int kPRcstSd = 5;
constexpr int kPCmntSd = 6;
constexpr int kPsQtySd = 7;
constexpr int kPsScstSd = 8;
constexpr int kPsCmntSd = 9;
constexpr int kOSuppSd = 10;
constexpr int kOClrkSd = 11;
constexpr int kOCmntSd = 12;
constexpr int kOOdateSd = 13;
constexpr int kLQtySd = 14;
constexpr int kLDcntSd = 15;
constexpr int kLTaxSd = 16;
constexpr int kLShipSd = 17;
constexpr int kLSmodeSd = 18;
constexpr int kLPkeySd = 19;
constexpr int kLSkeySd = 20;
constexpr int kLSdteSd = 21;
constexpr int kLCdteSd = 22;
constexpr int kLRdteSd = 23;
constexpr int kLRflgSd = 24;
constexpr int kLCmntSd = 25;
constexpr int kCAddrSd = 26;
constexpr int kCNtrgSd = 27;
constexpr int kCPhneSd = 28;
constexpr int kCAbalSd = 29;
constexpr int kCMsegSd = 30;
constexpr int kCCmntSd = 31;
constexpr int kSAddrSd = 32;
constexpr int kSNtrgSd = 33;
constexpr int kSPhneSd = 34;
constexpr int kSAbalSd = 35;
constexpr int kSCmntSd = 36;
constexpr int kPNameSd = 37;
constexpr int kOPrioSd = 38;
constexpr int kHVarSd = 39;
constexpr int kOCkeySd = 40;
constexpr int kNCmntSd = 41;
constexpr int kRCmntSd = 42;
constexpr int kOLcntSd = 43;
constexpr int kBbbOffsetSd = 44;
constexpr int kBbbTypeSd = 45;
constexpr int kBbbCmntSd = 46;
constexpr int kBbbJnkSd = 47;

constexpr int kPCatSd = 97;
constexpr int kCNatSd = 16;
constexpr int kCRegSd = 3;

}  // namespace benchgen::ssb::internal
