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

#include "util/record_batch_writer.h"

#include <cmath>
#include <cstdio>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <string>

#include "tpcds/utils/date.h"

namespace benchgen::internal {

arrow::Status RecordBatchWriter::Write(
    std::ostream* out, const std::shared_ptr<arrow::RecordBatch>& batch) const {
  if (out == nullptr) {
    return arrow::Status::Invalid("output stream must not be null");
  }
  if (!batch) {
    return arrow::Status::Invalid("record batch must not be null");
  }
  return WriteInternal(out, *batch);
}

arrow::Status RecordBatchWriter::Write(std::ostream* out,
                                       const arrow::RecordBatch& batch) const {
  if (out == nullptr) {
    return arrow::Status::Invalid("output stream must not be null");
  }
  return WriteInternal(out, batch);
}

arrow::Status RecordBatchWriter::WriteInternal(
    std::ostream* out, const arrow::RecordBatch& batch) const {
  const int num_columns = batch.num_columns();
  for (int64_t row = 0; row < batch.num_rows(); ++row) {
    std::string line;
    for (int col = 0; col < num_columns; ++col) {
      ARROW_RETURN_NOT_OK(AppendValue(&line, batch.column(col), row));
      line.push_back('|');
    }
    line.push_back('\n');
    *out << line;
  }
  return arrow::Status::OK();
}

arrow::Status RecordBatchWriter::AppendValue(
    std::string* out, const std::shared_ptr<arrow::Array>& array,
    int64_t row) const {
  if (array == nullptr) {
    return arrow::Status::Invalid("array must not be null");
  }

  switch (array->type_id()) {
    case arrow::Type::INT32:
      AppendInt32(out, std::static_pointer_cast<arrow::Int32Array>(array), row);
      return arrow::Status::OK();
    case arrow::Type::INT64:
      AppendInt64(out, std::static_pointer_cast<arrow::Int64Array>(array), row);
      return arrow::Status::OK();
    case arrow::Type::STRING:
      AppendString(out, std::static_pointer_cast<arrow::StringArray>(array),
                   row);
      return arrow::Status::OK();
    case arrow::Type::BOOL:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendBool(out, std::static_pointer_cast<arrow::BooleanArray>(array),
                   row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::FLOAT:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendFloat(out, std::static_pointer_cast<arrow::FloatArray>(array),
                    row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::DOUBLE:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendDouble(out, std::static_pointer_cast<arrow::DoubleArray>(array),
                     row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::DECIMAL32:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendDecimal32(
            out, std::static_pointer_cast<arrow::Decimal32Array>(array), row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::DECIMAL64:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendDecimal64(
            out, std::static_pointer_cast<arrow::Decimal64Array>(array), row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::DECIMAL128:
      if (format_ == RecordBatchWriterFormat::kTpch ||
          format_ == RecordBatchWriterFormat::kTpcds) {
        AppendDecimal128(
            out, std::static_pointer_cast<arrow::Decimal128Array>(array), row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::DECIMAL256:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendDecimal256(
            out, std::static_pointer_cast<arrow::Decimal256Array>(array), row);
        return arrow::Status::OK();
      }
      break;
    case arrow::Type::DATE32:
      if (format_ == RecordBatchWriterFormat::kTpcds) {
        AppendDate32(out, std::static_pointer_cast<arrow::Date32Array>(array),
                     row);
        return arrow::Status::OK();
      }
      break;
    default:
      break;
  }

  if (format_ == RecordBatchWriterFormat::kTpch) {
    if (array->IsNull(row)) {
      return arrow::Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(auto scalar, array->GetScalar(row));
    if (scalar->is_valid) {
      out->append(scalar->ToString());
    }
    return arrow::Status::OK();
  }

  return arrow::Status::NotImplemented("unsupported column type: " +
                                       array->type()->ToString());
}

void RecordBatchWriter::AppendInt32(
    std::string* out, const std::shared_ptr<arrow::Int32Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->append(std::to_string(array->Value(row)));
  }
}

void RecordBatchWriter::AppendInt64(
    std::string* out, const std::shared_ptr<arrow::Int64Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->append(std::to_string(array->Value(row)));
  }
}

void RecordBatchWriter::AppendString(
    std::string* out, const std::shared_ptr<arrow::StringArray>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    auto view = array->GetView(row);
    out->append(view.data(), view.size());
  }
}

void RecordBatchWriter::AppendBool(
    std::string* out, const std::shared_ptr<arrow::BooleanArray>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->push_back(array->Value(row) ? 'Y' : 'N');
  }
}

void RecordBatchWriter::AppendFloatValue(std::string* out, double value) const {
  double rounded = std::nearbyint(value);
  if (std::fabs(value - rounded) < 1e-6) {
    out->append(std::to_string(static_cast<int64_t>(rounded)));
    return;
  }
  std::ostringstream stream;
  stream << std::setprecision(6) << std::defaultfloat << value;
  out->append(stream.str());
}

void RecordBatchWriter::AppendFloat(
    std::string* out, const std::shared_ptr<arrow::FloatArray>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    AppendFloatValue(out, array->Value(row));
  }
}

void RecordBatchWriter::AppendDouble(
    std::string* out, const std::shared_ptr<arrow::DoubleArray>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    AppendFloatValue(out, array->Value(row));
  }
}

void RecordBatchWriter::AppendDecimal32(
    std::string* out, const std::shared_ptr<arrow::Decimal32Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->append(array->FormatValue(row));
  }
}

void RecordBatchWriter::AppendDecimal64(
    std::string* out, const std::shared_ptr<arrow::Decimal64Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->append(array->FormatValue(row));
  }
}

void RecordBatchWriter::AppendDecimal128(
    std::string* out, const std::shared_ptr<arrow::Decimal128Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->append(array->FormatValue(row));
  }
}

void RecordBatchWriter::AppendDecimal256(
    std::string* out, const std::shared_ptr<arrow::Decimal256Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    out->append(array->FormatValue(row));
  }
}

void RecordBatchWriter::AppendDate32(
    std::string* out, const std::shared_ptr<arrow::Date32Array>& array,
    int64_t row) const {
  if (!array->IsNull(row)) {
    int32_t days_since_epoch = array->Value(row);
    static const int kEpochJulian =
        benchgen::tpcds::internal::Date::ToJulianDays({1970, 1, 1});
    benchgen::tpcds::internal::Date date =
        benchgen::tpcds::internal::Date::FromJulianDays(kEpochJulian +
                                                        days_since_epoch);
    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", date.year, date.month,
             date.day);
    out->append(buf);
  }
}

}  // namespace benchgen::internal
