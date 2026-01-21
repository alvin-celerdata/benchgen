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

#include "distribution/scaling.h"
#include "utils/utils.h"

namespace benchgen::tpch::internal {
namespace {

arrow::Status PopulateDistributions(const DistributionStore& store,
                                    DbgenDistributions* distributions) {
  distributions->p_cntr = store.Find("p_cntr");
  distributions->colors = store.Find("colors");
  distributions->p_types = store.Find("p_types");
  distributions->nations = store.Find("nations");
  distributions->regions = store.Find("regions");
  distributions->o_priority = store.Find("o_oprio");
  distributions->l_instruct = store.Find("instruct");
  distributions->l_smode = store.Find("smode");
  distributions->l_category = store.Find("category");
  distributions->l_rflag = store.Find("rflag");
  distributions->c_mseg = store.Find("msegmnt");
  distributions->nouns = store.Find("nouns");
  distributions->verbs = store.Find("verbs");
  distributions->adjectives = store.Find("adjectives");
  distributions->adverbs = store.Find("adverbs");
  distributions->auxillaries = store.Find("auxillaries");
  distributions->terminators = store.Find("terminators");
  distributions->articles = store.Find("articles");
  distributions->prepositions = store.Find("prepositions");
  distributions->grammar = store.Find("grammar");
  distributions->np = store.Find("np");
  distributions->vp = store.Find("vp");

  auto missing = [](const Distribution* dist) { return dist == nullptr; };
  if (missing(distributions->p_cntr) || missing(distributions->colors) ||
      missing(distributions->p_types) || missing(distributions->nations) ||
      missing(distributions->regions) || missing(distributions->o_priority) ||
      missing(distributions->l_instruct) || missing(distributions->l_smode) ||
      missing(distributions->l_category) || missing(distributions->l_rflag) ||
      missing(distributions->c_mseg) || missing(distributions->nouns) ||
      missing(distributions->verbs) || missing(distributions->adjectives) ||
      missing(distributions->adverbs) || missing(distributions->auxillaries) ||
      missing(distributions->terminators) || missing(distributions->articles) ||
      missing(distributions->prepositions) || missing(distributions->grammar) ||
      missing(distributions->np) || missing(distributions->vp)) {
    return arrow::Status::Invalid("missing distributions in dists.dss");
  }
  return arrow::Status::OK();
}

arrow::Status CheckRowCount(const char* name, int64_t rows, int64_t* out) {
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

arrow::Status DbgenContext::Init(double scale_factor) {
  if (scale_factor <= 0.0) {
    return arrow::Status::Invalid("scale_factor must be positive");
  }

  auto status = provider_.Init();
  if (!status.ok()) {
    return status;
  }
  if (initialized_) {
    return arrow::Status::OK();
  }
  status = PopulateDistributions(provider_.store(), &distributions_);
  if (!status.ok()) {
    provider_ = DistributionProvider();
    return status;
  }
  BuildAscDate(&asc_date_);

  initialized_ = true;
  return arrow::Status::OK();
}

arrow::Status AdvanceSeedsForTable(RandomState* rng, TableId table,
                                   double scale_factor,
                                   const DbgenDistributions& distributions) {
  if (!rng) {
    return arrow::Status::Invalid("random state must be provided");
  }

  int64_t part_rows = 0;
  int64_t supp_rows = 0;
  int64_t cust_rows = 0;
  int64_t order_rows = 0;

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
  status = CheckRowCount("orders", OrderCount(scale_factor), &order_rows);
  if (!status.ok()) {
    return status;
  }

  auto skip_nation = [&]() {
    if (!distributions.nations) {
      return;
    }
    rng->AdvanceStream(
        kNCmntSd, rng->SeedBoundary(kNCmntSd) *
                      static_cast<int64_t>(distributions.nations->list.size()));
  };

  switch (table) {
    case TableId::kPart:
    case TableId::kPartSupp:
      return arrow::Status::OK();
    case TableId::kSupplier:
      SkipPart(rng, part_rows);
      SkipPartSupp(rng, part_rows);
      return arrow::Status::OK();
    case TableId::kCustomer:
      SkipPart(rng, part_rows);
      SkipPartSupp(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      return arrow::Status::OK();
    case TableId::kOrders:
      SkipPart(rng, part_rows);
      SkipPartSupp(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      SkipCustomer(rng, cust_rows);
      return arrow::Status::OK();
    case TableId::kLineItem:
      SkipPart(rng, part_rows);
      SkipPartSupp(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      SkipCustomer(rng, cust_rows);
      return arrow::Status::OK();
    case TableId::kNation:
      SkipPart(rng, part_rows);
      SkipPartSupp(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      SkipCustomer(rng, cust_rows);
      SkipOrder(rng, order_rows);
      SkipLine(rng, order_rows, false);
      return arrow::Status::OK();
    case TableId::kRegion:
      SkipPart(rng, part_rows);
      SkipPartSupp(rng, part_rows);
      SkipSupplier(rng, supp_rows);
      SkipCustomer(rng, cust_rows);
      SkipOrder(rng, order_rows);
      SkipLine(rng, order_rows, false);
      skip_nation();
      return arrow::Status::OK();
    case TableId::kTableCount:
      break;
  }
  return arrow::Status::OK();
}

}  // namespace benchgen::tpch::internal
