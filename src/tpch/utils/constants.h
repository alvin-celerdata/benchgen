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

namespace benchgen::tpch::internal {

enum class DbgenTable : int {
  kNone = -1,
  kPart = 0,
  kPartSupp = 1,
  kSupplier = 2,
  kCustomer = 3,
  kOrders = 4,
  kLineItem = 5,
  kOrderLine = 6,
  kPartPsupp = 7,
  kNation = 8,
  kRegion = 9,
  kUpdate = 10,
};

constexpr int kMaxStream = 47;
constexpr int64_t kMaxLong = 0x7fffffff;
constexpr int kMaxGrammarLen = 12;
constexpr int kMaxSentenceLen = 256;
constexpr int kMaxAggLen = 20;
constexpr int kMaxColor = 92;
constexpr int kMaxTextLen = 256;

constexpr double kVStrLow = 0.4;
constexpr double kVStrHigh = 1.6;

constexpr int kTextPoolSize = 300 * 1024 * 1024;
constexpr int kTextPoolStream = 5;

constexpr int kNCommentLen = 72;
constexpr int kNCommentMax = 152;
constexpr int kRCommentLen = 72;
constexpr int kRCommentMax = 152;
constexpr int kPMfgLen = 25;
constexpr int kPNameLen = 55;
constexpr int kPBrandLen = 10;
constexpr int kPTypeLen = 25;
constexpr int kPContainerLen = 10;
constexpr int kPCommentLen = 14;
constexpr int kPCommentMax = 23;
constexpr int kSNameLen = 25;
constexpr int kSAddressLen = 25;
constexpr int kSAddressMax = 40;
constexpr int kSCommentLen = 63;
constexpr int kSCommentMax = 101;
constexpr int kPSCommentLen = 124;
constexpr int kPSCommentMax = 199;
constexpr int kCNameLen = 18;
constexpr int kCAddressLen = 25;
constexpr int kCAddressMax = 40;
constexpr int kCMsegLen = 10;
constexpr int kCCommentLen = 73;
constexpr int kCCommentMax = 117;
constexpr int kOPrioLen = 15;
constexpr int kOClerkLen = 15;
constexpr int kOCommentLen = 49;
constexpr int kOCommentMax = 79;
constexpr int kLCommentLen = 27;
constexpr int kLCommentMax = 44;
constexpr int kLInstrLen = 25;
constexpr int kLSmodeLen = 10;
constexpr int kDateLen = 13;
constexpr int kNationLen = 25;
constexpr int kRegionLen = 25;
constexpr int kPhoneLen = 15;

inline constexpr const char kPMfgTag[] = "Manufacturer#";
inline constexpr const char kPBrndTag[] = "Brand#";
inline constexpr const char kSNameTag[] = "Supplier#";
inline constexpr const char kCNameTag[] = "Customer#";
inline constexpr const char kOClerkTag[] = "Clerk#";

constexpr int64_t kPMfgMin = 1;
constexpr int64_t kPMfgMax = 5;
constexpr int64_t kPBrndMin = 1;
constexpr int64_t kPBrndMax = 5;
constexpr int64_t kPSizeMin = 1;
constexpr int64_t kPSizeMax = 50;
constexpr int64_t kPSScostMin = 100;
constexpr int64_t kPSScostMax = 100000;
constexpr int64_t kPSQtyMin = 1;
constexpr int64_t kPSQtyMax = 9999;
constexpr int64_t kSAbalMin = -99999;
constexpr int64_t kSAbalMax = 999999;
constexpr int64_t kCAbalMin = -99999;
constexpr int64_t kCAbalMax = 999999;
constexpr int64_t kCMsegMax = 5;
constexpr int64_t kOClerkScale = 1000;
constexpr int64_t kLQtyMin = 1;
constexpr int64_t kLQtyMax = 50;
constexpr int64_t kLTaxMin = 0;
constexpr int64_t kLTaxMax = 8;
constexpr int64_t kLDiscMin = 0;
constexpr int64_t kLDiscMax = 10;
constexpr int64_t kLSdteMin = 1;
constexpr int64_t kLSdteMax = 121;
constexpr int64_t kLCdteMin = 30;
constexpr int64_t kLCdteMax = 90;
constexpr int64_t kLRdteMin = 1;
constexpr int64_t kLRdteMax = 30;

constexpr int64_t kPNameScl = 5;
constexpr int64_t kSCommentBbb = 10;
constexpr int64_t kBbbDeadbeats = 50;
constexpr int64_t kNationsMax = 90;

inline constexpr const char kBbbBase[] = "Customer ";
inline constexpr const char kBbbComplain[] = "Complaints";
inline constexpr const char kBbbCommend[] = "Recommends";
constexpr int64_t kBbbBaseLen = 9;
constexpr int64_t kBbbTypeLen = 10;
constexpr int64_t kBbbCommentLen = 19;

constexpr int64_t kStartDate = 92001;
constexpr int64_t kCurrentDate = 95168;
constexpr int64_t kEndDate = 98365;
constexpr int64_t kTotalDate = 2557;
constexpr int64_t kPennies = 100;

constexpr int kSparseBits = 2;
constexpr int kSparseKeep = 3;

constexpr int kOrdersPerCustomer = 10;
constexpr int64_t kCustomerMortality = 3;
constexpr int kSuppPerPart = 4;
constexpr int kOLcntMin = 1;
constexpr int kOLcntMax = 7;

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
constexpr int kBbbJnkSd = 44;
constexpr int kBbbTypeSd = 45;
constexpr int kBbbCmntSd = 46;
constexpr int kBbbOffsetSd = 47;

}  // namespace benchgen::tpch::internal
