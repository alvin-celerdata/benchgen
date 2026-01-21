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
#include <string>

#include "distribution/scaling.h"
#include "utils/random_number_stream.h"
#include "utils/tables.h"

namespace benchgen::tpcds::internal {

struct ScdDates {
  int min_date = 0;
  int max_date = 0;
  int half_date = 0;
  int third_date = 0;
  int two_third_date = 0;
};

const ScdDates& GetScdDates();

struct ScdState {
  std::array<std::string, MAX_TABLE + 1> business_keys;
};

bool SetSCDKeys(int column_id, int64_t index, std::string* business_key,
                int32_t* rec_start_date_id, int32_t* rec_end_date_id,
                ScdState* state);

inline bool SetSCDKeys(int column_id, int64_t index, std::string* business_key,
                       int32_t* rec_start_date_id, int32_t* rec_end_date_id) {
  return SetSCDKeys(column_id, index, business_key, rec_start_date_id,
                    rec_end_date_id, nullptr);
}

int64_t ScdGroupStartRow(int64_t row_number);

int64_t MatchSCDSK(int64_t unique_id, int64_t julian_date, int table_number,
                   const Scaling& scaling);

int64_t ScdJoin(int table_number, int column_id, int64_t julian_date,
                RandomNumberStream* stream, const Scaling& scaling);

int64_t GetSKFromID(int64_t id, int column_id, RandomNumberStream* stream);
int64_t GetFirstSK(int64_t id);

template <typename T>
void ChangeSCDValue(T* new_value, T* old_value, int* flags, bool first_record) {
  if (new_value == nullptr || old_value == nullptr || flags == nullptr) {
    return;
  }
  bool keep_old = ((*flags & 1) != 0) && !first_record;
  if (keep_old) {
    *new_value = *old_value;
  } else {
    *old_value = *new_value;
  }
  *flags /= 2;
}

inline void ConsumeSCDFlag(int* flags) {
  if (flags == nullptr) {
    return;
  }
  // Match tpcds-kit SCD_PTR behavior: consume the flag without copying values.
  *flags /= 2;
}

template <typename T>
void ChangeSCDValuePtr(T* new_value, T* old_value, int* flags,
                       bool /*first_record*/) {
  (void)new_value;
  (void)old_value;
  ConsumeSCDFlag(flags);
}

}  // namespace benchgen::tpcds::internal
