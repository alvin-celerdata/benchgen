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

#include "utils/context.h"

#include <limits>
#include <string>

#include "utils/random.h"
#include "utils/scaling.h"
#include "utils/utils.h"

namespace benchgen::ssb::internal {
namespace {

arrow::Status CheckRowCount(const std::string& name, int64_t rows,
                            int64_t* out) {
  if (rows < 0) {
    return arrow::Status::Invalid("failed to compute row count for ", name);
  }
  if (rows > std::numeric_limits<long>::max()) {
    return arrow::Status::Invalid("row count overflow for ", name);
  }
  *out = rows;
  return arrow::Status::OK();
}

}  // namespace

arrow::Status DbgenDistributions::Init(const DistributionStore& store) {
  p_cntr = store.Find("p_cntr");
  colors = store.Find("colors");
  p_types = store.Find("p_types");
  nations = store.Find("nations");
  regions = store.Find("regions");
  o_priority = store.Find("o_oprio");
  l_instruct = store.Find("instruct");
  l_smode = store.Find("smode");
  l_category = store.Find("category");
  l_rflag = store.Find("rflag");
  c_mseg = store.Find("msegmnt");
  nouns = store.Find("nouns");
  verbs = store.Find("verbs");
  adjectives = store.Find("adjectives");
  adverbs = store.Find("adverbs");
  auxillaries = store.Find("auxillaries");
  terminators = store.Find("terminators");
  articles = store.Find("articles");
  prepositions = store.Find("prepositions");
  grammar = store.Find("grammar");
  np = store.Find("np");
  vp = store.Find("vp");

  auto missing = [](const Distribution* dist) { return dist == nullptr; };
  if (missing(p_cntr) || missing(colors) || missing(p_types) ||
      missing(nations) || missing(regions) || missing(o_priority) ||
      missing(l_instruct) || missing(l_smode) || missing(l_category) ||
      missing(l_rflag) || missing(c_mseg) || missing(nouns) ||
      missing(verbs) || missing(adjectives) || missing(adverbs) ||
      missing(auxillaries) || missing(terminators) || missing(articles) ||
      missing(prepositions) || missing(grammar) || missing(np) ||
      missing(vp)) {
    return arrow::Status::Invalid("missing distributions in dists.dss");
  }
  return arrow::Status::OK();
}

arrow::Status DbgenContext::Init(double scale_factor) {
  if (scale_factor <= 0.0) {
    return arrow::Status::Invalid("scale_factor must be positive");
  }
  if (scale_factor < 1.0) {
    return arrow::Status::Invalid("scale_factor must be >= 1.0 for SSB");
  }

  auto status = provider_.Init();
  if (!status.ok()) {
    return status;
  }
  if (initialized_) {
    return arrow::Status::OK();
  }

  status = distributions_.Init(provider_.store());
  if (!status.ok()) {
    provider_ = DistributionProvider();
    return status;
  }
  BuildAscDate(&asc_date_);

  initialized_ = true;
  return arrow::Status::OK();
}

arrow::Status AdvanceSeedsForTable(RandomState* rng, TableId table,
                                   double scale_factor) {
  if (!rng) {
    return arrow::Status::Invalid("random state must be provided");
  }

  int64_t part_rows = 0;
  int64_t supp_rows = 0;
  int64_t cust_rows = 0;
  int64_t date_rows = 0;

  auto status =
      CheckRowCount("part", RowCount(TableId::kPart, scale_factor), &part_rows);
  if (!status.ok()) {
    return status;
  }
  status = CheckRowCount("supplier", RowCount(TableId::kSupplier, scale_factor),
                         &supp_rows);
  if (!status.ok()) {
    return status;
  }
  status = CheckRowCount("customer", RowCount(TableId::kCustomer, scale_factor),
                         &cust_rows);
  if (!status.ok()) {
    return status;
  }
  status =
      CheckRowCount("date", RowCount(TableId::kDate, scale_factor), &date_rows);
  if (!status.ok()) {
    return status;
  }

  switch (table) {
    case TableId::kPart:
      return arrow::Status::OK();
    case TableId::kSupplier:
      SkipPart(rng, part_rows);
      return arrow::Status::OK();
    case TableId::kCustomer:
      SkipPart(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      return arrow::Status::OK();
    case TableId::kDate:
      SkipPart(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      SkipCustomer(rng, cust_rows);
      return arrow::Status::OK();
    case TableId::kLineorder:
      SkipPart(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      SkipCustomer(rng, cust_rows);
      SkipOrder(rng, date_rows);
      return arrow::Status::OK();
    case TableId::kTableCount:
      break;
  }
  return arrow::Status::OK();
}

}  // namespace benchgen::ssb::internal
