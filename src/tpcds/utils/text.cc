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

#include "utils/text.h"

#include <cctype>
#include <stdexcept>

#include "utils/random_utils.h"

namespace benchgen::tpcds::internal {
namespace {

std::string PickWord(DstDistributionStore* store, const std::string& dist_name,
                     RandomNumberStream* stream) {
  const auto& dist = store->Get(dist_name);
  int index = dist.PickIndex(1, stream);
  return dist.GetString(index, 1);
}

std::string MakeSentence(DstDistributionStore* store,
                         RandomNumberStream* stream) {
  const auto& sentences = store->Get("sentences");
  int index = sentences.PickIndex(1, stream);
  std::string syntax = sentences.GetString(index, 1);

  std::string out;
  out.reserve(syntax.size() * 2);
  for (char c : syntax) {
    switch (c) {
      case 'N':
        out += PickWord(store, "nouns", stream);
        break;
      case 'V':
        out += PickWord(store, "verbs", stream);
        break;
      case 'J':
        out += PickWord(store, "adjectives", stream);
        break;
      case 'D':
        out += PickWord(store, "adverbs", stream);
        break;
      case 'X':
        out += PickWord(store, "auxiliaries", stream);
        break;
      case 'P':
        out += PickWord(store, "prepositions", stream);
        break;
      case 'A':
        out += PickWord(store, "articles", stream);
        break;
      case 'T':
        out += PickWord(store, "terminators", stream);
        break;
      default:
        out.push_back(c);
        break;
    }
  }
  return out;
}

}  // namespace

std::string GenerateText(int min, int max, DstDistributionStore* store,
                         RandomNumberStream* stream) {
  if (store == nullptr || stream == nullptr) {
    throw std::invalid_argument("text generation requires store and stream");
  }

  int target_len = GenerateUniformRandomInt(min, max, stream);
  std::string out;
  out.reserve(static_cast<size_t>(target_len));

  bool capitalize = true;
  while (target_len > 0) {
    std::string sentence = MakeSentence(store, stream);
    int generated_length = static_cast<int>(sentence.size());
    if (capitalize && !sentence.empty()) {
      sentence[0] = static_cast<char>(
          std::toupper(static_cast<unsigned char>(sentence[0])));
    }
    if (!sentence.empty() && sentence.back() == '.') {
      capitalize = true;
    } else {
      capitalize = false;
    }

    if (target_len <= generated_length) {
      sentence.resize(static_cast<size_t>(target_len));
    }

    out += sentence;
    target_len -= generated_length;
    if (target_len > 0) {
      out.push_back(' ');
      target_len -= 1;
    }
  }

  return out;
}

}  // namespace benchgen::tpcds::internal
