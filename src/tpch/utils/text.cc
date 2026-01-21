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

#include <algorithm>
#include <cstring>
#include <mutex>
#include <string_view>

#include "utils/constants.h"

namespace benchgen::tpch::internal {
namespace {

int PickString(const Distribution& dist, int stream, RandomState* rng,
               char* target) {
  if (!rng || dist.list.empty() || !target || dist.max <= 0) {
    return -1;
  }
  int64_t pick = rng->RandomInt(1, dist.max, stream);
  std::size_t i = 0;
  while (dist.list[i].weight < pick) {
    ++i;
  }
  std::strcpy(target, dist.list[i].text.c_str());
  return static_cast<int>(i);
}

int TextVerbPhrase(char* dest, const DbgenDistributions& dists, int stream,
                   RandomState* rng) {
  char syntax[kMaxGrammarLen + 1] = {};
  if (PickString(*dists.vp, stream, rng, syntax) < 0) {
    return 0;
  }

  int res = 0;
  char* token = std::strtok(syntax, " ");
  while (token) {
    const Distribution* src = nullptr;
    switch (*token) {
      case 'D':
        src = dists.adverbs;
        break;
      case 'V':
        src = dists.verbs;
        break;
      case 'X':
        src = dists.auxillaries;
        break;
      default:
        break;
    }
    if (!src) {
      token = std::strtok(nullptr, " ");
      continue;
    }
    int idx = PickString(*src, stream, rng, dest);
    if (idx < 0) {
      token = std::strtok(nullptr, " ");
      continue;
    }
    int len = static_cast<int>(std::strlen(src->list[idx].text.c_str()));
    dest += len;
    res += len;
    if (token[1] != '\0') {
      *dest = token[1];
      ++dest;
      ++res;
    }
    *dest = ' ';
    ++dest;
    ++res;
    token = std::strtok(nullptr, " ");
  }
  return res;
}

int TextNounPhrase(char* dest, const DbgenDistributions& dists, int stream,
                   RandomState* rng) {
  char syntax[kMaxGrammarLen + 1] = {};
  if (PickString(*dists.np, stream, rng, syntax) < 0) {
    return 0;
  }

  int res = 0;
  char* token = std::strtok(syntax, " ");
  while (token) {
    const Distribution* src = nullptr;
    switch (*token) {
      case 'A':
        src = dists.articles;
        break;
      case 'J':
        src = dists.adjectives;
        break;
      case 'D':
        src = dists.adverbs;
        break;
      case 'N':
        src = dists.nouns;
        break;
      default:
        break;
    }
    if (!src) {
      token = std::strtok(nullptr, " ");
      continue;
    }
    int idx = PickString(*src, stream, rng, dest);
    if (idx < 0) {
      token = std::strtok(nullptr, " ");
      continue;
    }
    int len = static_cast<int>(std::strlen(src->list[idx].text.c_str()));
    dest += len;
    res += len;
    if (token[1] != '\0') {
      *dest = token[1];
      ++dest;
      ++res;
    }
    *dest = ' ';
    ++dest;
    ++res;
    token = std::strtok(nullptr, " ");
  }
  return res;
}

int TextSentence(char* dest, const DbgenDistributions& dists, int stream,
                 RandomState* rng) {
  char syntax[kMaxGrammarLen + 1] = {};
  if (PickString(*dists.grammar, stream, rng, syntax) < 0) {
    return -1;
  }

  char* cursor = syntax;
  int res = 0;
  int len = 0;

  while (true) {
    while (*cursor && *cursor == ' ') {
      ++cursor;
    }
    if (*cursor == '\0') {
      break;
    }
    switch (*cursor) {
      case 'V':
        len = TextVerbPhrase(dest, dists, stream, rng);
        break;
      case 'N':
        len = TextNounPhrase(dest, dists, stream, rng);
        break;
      case 'P': {
        int idx = PickString(*dists.prepositions, stream, rng, dest);
        if (idx < 0) {
          len = 0;
          break;
        }
        len = static_cast<int>(
            std::strlen(dists.prepositions->list[idx].text.c_str()));
        std::strcpy(dest + len, " the ");
        len += 5;
        len += TextNounPhrase(dest + len, dists, stream, rng);
        break;
      }
      case 'T': {
        char* term_target = dest - 1;
        int idx = PickString(*dists.terminators, stream, rng, term_target);
        if (idx < 0) {
          len = 0;
          break;
        }
        dest = term_target;
        len = static_cast<int>(
            std::strlen(dists.terminators->list[idx].text.c_str()));
        break;
      }
      default:
        len = 0;
        break;
    }
    dest += len;
    res += len;
    ++cursor;
    if (*cursor && *cursor != ' ') {
      *dest = *cursor;
      ++dest;
      ++res;
    }
  }

  *dest = '\0';
  return --res;
}

class TextPool {
 public:
  TextPool() = default;

  void Build(const DbgenDistributions& dists) {
    pool_.assign(static_cast<std::size_t>(kTextPoolSize), '\0');
    RandomState rng;
    rng.Reset();

    std::size_t offset = 0;
    char sentence[kMaxSentenceLen + 1] = {};
    while (offset < pool_.size()) {
      int length = TextSentence(sentence, dists, kTextPoolStream, &rng);
      if (length < 0) {
        break;
      }
      std::size_t needed = pool_.size() - offset;
      if (needed >= static_cast<std::size_t>(length + 1)) {
        std::memcpy(&pool_[offset], sentence, static_cast<std::size_t>(length));
        offset += static_cast<std::size_t>(length);
        pool_[offset] = ' ';
        ++offset;
      } else {
        std::memcpy(&pool_[offset], sentence, needed);
        offset += needed;
      }
    }
  }

  std::string_view Slice(int64_t offset, int64_t length) const {
    return std::string_view(pool_.data() + offset,
                            static_cast<std::size_t>(length));
  }

  int64_t size() const { return static_cast<int64_t>(pool_.size()); }

 private:
  std::string pool_;
};

const TextPool& GetTextPool(const DbgenDistributions& dists) {
  static std::mutex mutex;
  static TextPool pool;
  static bool initialized = false;

  std::lock_guard<std::mutex> lock(mutex);
  if (!initialized) {
    pool.Build(dists);
    initialized = true;
  }
  return pool;
}

}  // namespace

int GenerateText(int avg_length, int stream, RandomState* rng,
                 const DbgenDistributions& distributions, std::string* out) {
  if (!rng || !out) {
    return 0;
  }
  int min_len = static_cast<int>(avg_length * kVStrLow);
  int max_len = static_cast<int>(avg_length * kVStrHigh);
  if (min_len < 0) {
    min_len = 0;
  }
  if (max_len < min_len) {
    max_len = min_len;
  }

  const TextPool& pool = GetTextPool(distributions);
  if (pool.size() <= max_len) {
    out->clear();
    return 0;
  }

  int64_t offset = rng->RandomInt(0, pool.size() - max_len, stream);
  int64_t length = rng->RandomInt(min_len, max_len, stream);
  std::string_view slice = pool.Slice(offset, length);
  out->assign(slice.data(), slice.size());
  return static_cast<int>(length);
}

}  // namespace benchgen::tpch::internal
