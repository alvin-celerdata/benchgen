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

#include <iosfwd>
#include <memory>
#include <string>

#include "benchgen/arrow_compat.h"

namespace benchgen::internal {

enum class RecordBatchWriterFormat {
  kTpch,
  kTpcds,
  kSsb,
};

class RecordBatchWriter {
 public:
  explicit RecordBatchWriter(RecordBatchWriterFormat format)
      : format_(format) {}

  arrow::Status Write(std::ostream* out,
                      const std::shared_ptr<arrow::RecordBatch>& batch) const;
  arrow::Status Write(std::ostream* out, const arrow::RecordBatch& batch) const;

 private:
  arrow::Status WriteInternal(std::ostream* out,
                              const arrow::RecordBatch& batch) const;
  arrow::Status AppendValue(std::string* out,
                            const std::shared_ptr<arrow::Array>& array,
                            int64_t row) const;

  void AppendInt32(std::string* out,
                   const std::shared_ptr<arrow::Int32Array>& array,
                   int64_t row) const;
  void AppendInt64(std::string* out,
                   const std::shared_ptr<arrow::Int64Array>& array,
                   int64_t row) const;
  void AppendString(std::string* out,
                    const std::shared_ptr<arrow::StringArray>& array,
                    int64_t row) const;
  void AppendBool(std::string* out,
                  const std::shared_ptr<arrow::BooleanArray>& array,
                  int64_t row) const;
  void AppendFloatValue(std::string* out, double value) const;
  void AppendFloat(std::string* out,
                   const std::shared_ptr<arrow::FloatArray>& array,
                   int64_t row) const;
  void AppendDouble(std::string* out,
                    const std::shared_ptr<arrow::DoubleArray>& array,
                    int64_t row) const;
  void AppendDecimal32(std::string* out,
                       const std::shared_ptr<arrow::Decimal32Array>& array,
                       int64_t row) const;
  void AppendDecimal64(std::string* out,
                       const std::shared_ptr<arrow::Decimal64Array>& array,
                       int64_t row) const;
  void AppendDecimal128(std::string* out,
                        const std::shared_ptr<arrow::Decimal128Array>& array,
                        int64_t row) const;
  void AppendDecimal256(std::string* out,
                        const std::shared_ptr<arrow::Decimal256Array>& array,
                        int64_t row) const;
  void AppendDate32(std::string* out,
                    const std::shared_ptr<arrow::Date32Array>& array,
                    int64_t row) const;

  RecordBatchWriterFormat format_;
};

}  // namespace benchgen::internal
