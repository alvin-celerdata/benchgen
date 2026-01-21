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

#include "md5.h"

#include <array>
#include <cstring>
#include <sstream>

namespace {

constexpr std::array<uint32_t, 64> kK = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a,
    0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340,
    0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
    0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92,
    0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
};

constexpr std::array<uint32_t, 64> kS = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5, 9,  14, 20, 5, 9,  14, 20, 5, 9,  14, 20, 5, 9,  14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
};

inline uint32_t LeftRotate(uint32_t x, uint32_t c) {
  return (x << c) | (x >> (32 - c));
}

void Encode(uint8_t* output, const uint32_t* input, size_t length) {
  for (size_t i = 0, j = 0; j < length; ++i, j += 4) {
    output[j] = static_cast<uint8_t>(input[i] & 0xff);
    output[j + 1] = static_cast<uint8_t>((input[i] >> 8) & 0xff);
    output[j + 2] = static_cast<uint8_t>((input[i] >> 16) & 0xff);
    output[j + 3] = static_cast<uint8_t>((input[i] >> 24) & 0xff);
  }
}

void Decode(uint32_t* output, const uint8_t* input, size_t length) {
  for (size_t i = 0, j = 0; j < length; ++i, j += 4) {
    output[i] = static_cast<uint32_t>(input[j]) |
                (static_cast<uint32_t>(input[j + 1]) << 8) |
                (static_cast<uint32_t>(input[j + 2]) << 16) |
                (static_cast<uint32_t>(input[j + 3]) << 24);
  }
}

}  // namespace

Md5::Md5() {
  state_[0] = 0x67452301;
  state_[1] = 0xefcdab89;
  state_[2] = 0x98badcfe;
  state_[3] = 0x10325476;
  std::memset(buffer_, 0, sizeof(buffer_));
}

void Md5::Update(const void* data, size_t length) {
  if (finalized_) {
    return;
  }

  const uint8_t* input = static_cast<const uint8_t*>(data);
  size_t index = static_cast<size_t>((bit_count_ / 8) % 64);
  bit_count_ += static_cast<uint64_t>(length) * 8;

  size_t part_len = 64 - index;
  size_t i = 0;
  if (length >= part_len) {
    std::memcpy(&buffer_[index], input, part_len);
    Transform(buffer_);
    for (i = part_len; i + 63 < length; i += 64) {
      Transform(&input[i]);
    }
    index = 0;
  }
  std::memcpy(&buffer_[index], &input[i], length - i);
}

void Md5::Update(std::string_view data) { Update(data.data(), data.size()); }

std::string Md5::Final() {
  if (finalized_) {
    return "";
  }

  uint8_t padding[64] = {0x80};
  uint8_t length_bytes[8];
  uint32_t count[2] = {
      static_cast<uint32_t>(bit_count_ & 0xffffffff),
      static_cast<uint32_t>((bit_count_ >> 32) & 0xffffffff),
  };
  Encode(length_bytes, count, 8);

  size_t index = static_cast<size_t>((bit_count_ / 8) % 64);
  size_t pad_len = (index < 56) ? (56 - index) : (120 - index);
  Update(padding, pad_len);
  Update(length_bytes, 8);

  uint8_t digest[16];
  Encode(digest, state_, 16);

  std::ostringstream out;
  out.setf(std::ios::hex, std::ios::basefield);
  out.fill('0');
  for (uint8_t byte : digest) {
    out.width(2);
    out << static_cast<int>(byte);
  }

  finalized_ = true;
  return out.str();
}

void Md5::Transform(const uint8_t block[64]) {
  uint32_t a = state_[0];
  uint32_t b = state_[1];
  uint32_t c = state_[2];
  uint32_t d = state_[3];
  uint32_t x[16];
  Decode(x, block, 64);

  for (uint32_t i = 0; i < 64; ++i) {
    uint32_t f = 0;
    uint32_t g = 0;
    if (i < 16) {
      f = (b & c) | (~b & d);
      g = i;
    } else if (i < 32) {
      f = (d & b) | (~d & c);
      g = (5 * i + 1) % 16;
    } else if (i < 48) {
      f = b ^ c ^ d;
      g = (3 * i + 5) % 16;
    } else {
      f = c ^ (b | ~d);
      g = (7 * i) % 16;
    }
    uint32_t temp = d;
    d = c;
    c = b;
    b = b + LeftRotate(a + f + kK[i] + x[g], kS[i]);
    a = temp;
  }

  state_[0] += a;
  state_[1] += b;
  state_[2] += c;
  state_[3] += d;

  std::memset(x, 0, sizeof(x));
}

std::string Md5Hex(std::string_view input) {
  Md5 md5;
  md5.Update(input);
  return md5.Final();
}
