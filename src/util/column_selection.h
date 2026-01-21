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

#include <arrow/status.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "benchgen/arrow_compat.h"

namespace benchgen::internal {

class ColumnSelection {
 public:
  ColumnSelection() = default;

  arrow::Status Init(const std::shared_ptr<arrow::Schema>& full_schema,
                     const std::vector<std::string>& column_names);

  const std::shared_ptr<arrow::Schema>& schema() const { return schema_; }
  bool has_selection() const { return has_selection_; }

  arrow::Status MakeRecordBatch(
      int64_t num_rows, std::vector<std::shared_ptr<arrow::Array>> columns,
      std::shared_ptr<arrow::RecordBatch>* out) const;

 private:
  bool has_selection_ = false;
  int full_field_count_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<int> indices_;
};

inline arrow::Status ColumnSelection::Init(
    const std::shared_ptr<arrow::Schema>& full_schema,
    const std::vector<std::string>& column_names) {
  if (!full_schema) {
    return arrow::Status::Invalid("schema must not be null");
  }

  full_field_count_ = full_schema->num_fields();
  indices_.clear();
  schema_.reset();
  has_selection_ = false;

  if (column_names.empty()) {
    schema_ = full_schema;
    return arrow::Status::OK();
  }

  std::unordered_set<std::string> seen;
  indices_.reserve(column_names.size());
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(column_names.size());

  for (const auto& name : column_names) {
    if (!seen.insert(name).second) {
      return arrow::Status::Invalid("duplicate column name: " + name);
    }
    int index = full_schema->GetFieldIndex(name);
    if (index < 0) {
      return arrow::Status::Invalid("unknown column name: " + name);
    }
    indices_.push_back(index);
    fields.push_back(full_schema->field(index));
  }

  schema_ = arrow::schema(std::move(fields));
  has_selection_ = true;
  return arrow::Status::OK();
}

inline arrow::Status ColumnSelection::MakeRecordBatch(
    int64_t num_rows, std::vector<std::shared_ptr<arrow::Array>> columns,
    std::shared_ptr<arrow::RecordBatch>* out) const {
  if (out == nullptr) {
    return arrow::Status::Invalid("out record batch must not be null");
  }
  if (!schema_) {
    return arrow::Status::Invalid("column selection is not initialized");
  }

  if (!has_selection_) {
    if (columns.size() != static_cast<size_t>(schema_->num_fields())) {
      return arrow::Status::Invalid("column count does not match schema");
    }
    *out = arrow::RecordBatch::Make(schema_, num_rows, std::move(columns));
    return arrow::Status::OK();
  }

  if (columns.size() != static_cast<size_t>(full_field_count_)) {
    return arrow::Status::Invalid("column count does not match full schema");
  }

  std::vector<std::shared_ptr<arrow::Array>> selected;
  selected.reserve(indices_.size());
  for (int index : indices_) {
    if (index < 0 || index >= full_field_count_) {
      return arrow::Status::Invalid("selected column index out of range");
    }
    selected.push_back(columns[static_cast<size_t>(index)]);
  }

  *out = arrow::RecordBatch::Make(schema_, num_rows, std::move(selected));
  return arrow::Status::OK();
}

}  // namespace benchgen::internal
