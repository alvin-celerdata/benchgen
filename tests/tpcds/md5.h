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

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

class Md5 {
 public:
  Md5();

  void Update(const void* data, size_t length);
  void Update(std::string_view data);
  std::string Final();

 private:
  void Transform(const uint8_t block[64]);

  uint64_t bit_count_ = 0;
  uint32_t state_[4];
  uint8_t buffer_[64];
  bool finalized_ = false;
};

std::string Md5Hex(std::string_view input);
