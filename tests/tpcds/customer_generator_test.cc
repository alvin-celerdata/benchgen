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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "benchgen/arrow_compat.h"
#include "benchgen/record_batch_iterator_factory.h"
#include "md5.h"

namespace {

void AppendInt64(std::string* out,
                 const std::shared_ptr<arrow::Int64Array>& array, int64_t row) {
  if (!array->IsNull(row)) {
    out->append(std::to_string(array->Value(row)));
  }
}

void AppendInt32(std::string* out,
                 const std::shared_ptr<arrow::Int32Array>& array, int64_t row) {
  if (!array->IsNull(row)) {
    out->append(std::to_string(array->Value(row)));
  }
}

void AppendBool(std::string* out,
                const std::shared_ptr<arrow::BooleanArray>& array,
                int64_t row) {
  if (!array->IsNull(row)) {
    out->push_back(array->Value(row) ? 'Y' : 'N');
  }
}

void AppendString(std::string* out,
                  const std::shared_ptr<arrow::StringArray>& array,
                  int64_t row) {
  if (!array->IsNull(row)) {
    auto view = array->GetView(row);
    out->append(view.data(), view.size());
  }
}

void UpdateMd5FromBatch(Md5* md5,
                        const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto c_customer_sk =
      std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
  auto c_customer_id =
      std::static_pointer_cast<arrow::StringArray>(batch->column(1));
  auto c_current_cdemo_sk =
      std::static_pointer_cast<arrow::Int64Array>(batch->column(2));
  auto c_current_hdemo_sk =
      std::static_pointer_cast<arrow::Int64Array>(batch->column(3));
  auto c_current_addr_sk =
      std::static_pointer_cast<arrow::Int64Array>(batch->column(4));
  auto c_first_shipto_date_sk =
      std::static_pointer_cast<arrow::Int32Array>(batch->column(5));
  auto c_first_sales_date_sk =
      std::static_pointer_cast<arrow::Int32Array>(batch->column(6));
  auto c_salutation =
      std::static_pointer_cast<arrow::StringArray>(batch->column(7));
  auto c_first_name =
      std::static_pointer_cast<arrow::StringArray>(batch->column(8));
  auto c_last_name =
      std::static_pointer_cast<arrow::StringArray>(batch->column(9));
  auto c_preferred_cust_flag =
      std::static_pointer_cast<arrow::BooleanArray>(batch->column(10));
  auto c_birth_day =
      std::static_pointer_cast<arrow::Int32Array>(batch->column(11));
  auto c_birth_month =
      std::static_pointer_cast<arrow::Int32Array>(batch->column(12));
  auto c_birth_year =
      std::static_pointer_cast<arrow::Int32Array>(batch->column(13));
  auto c_birth_country =
      std::static_pointer_cast<arrow::StringArray>(batch->column(14));
  auto c_login =
      std::static_pointer_cast<arrow::StringArray>(batch->column(15));
  auto c_email_address =
      std::static_pointer_cast<arrow::StringArray>(batch->column(16));
  auto c_last_review_date_sk =
      std::static_pointer_cast<arrow::Int32Array>(batch->column(17));

  for (int64_t row = 0; row < batch->num_rows(); ++row) {
    std::string line;
    AppendInt64(&line, c_customer_sk, row);
    line.push_back('|');
    AppendString(&line, c_customer_id, row);
    line.push_back('|');
    AppendInt64(&line, c_current_cdemo_sk, row);
    line.push_back('|');
    AppendInt64(&line, c_current_hdemo_sk, row);
    line.push_back('|');
    AppendInt64(&line, c_current_addr_sk, row);
    line.push_back('|');
    AppendInt32(&line, c_first_shipto_date_sk, row);
    line.push_back('|');
    AppendInt32(&line, c_first_sales_date_sk, row);
    line.push_back('|');
    AppendString(&line, c_salutation, row);
    line.push_back('|');
    AppendString(&line, c_first_name, row);
    line.push_back('|');
    AppendString(&line, c_last_name, row);
    line.push_back('|');
    AppendBool(&line, c_preferred_cust_flag, row);
    line.push_back('|');
    AppendInt32(&line, c_birth_day, row);
    line.push_back('|');
    AppendInt32(&line, c_birth_month, row);
    line.push_back('|');
    AppendInt32(&line, c_birth_year, row);
    line.push_back('|');
    AppendString(&line, c_birth_country, row);
    line.push_back('|');
    AppendString(&line, c_login, row);
    line.push_back('|');
    AppendString(&line, c_email_address, row);
    line.push_back('|');
    AppendInt32(&line, c_last_review_date_sk, row);
    line.push_back('|');
    line.push_back('\n');
    md5->Update(line);
  }
}

struct CustomerMd5Result {
  std::string md5;
  int64_t row_count = 0;
};

CustomerMd5Result ComputeCustomerMd5(benchgen::RecordBatchIterator* iterator) {
  Md5 md5;
  CustomerMd5Result result;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    auto status = iterator->Next(&batch);
    EXPECT_TRUE(status.ok()) << status.ToString();
    if (!status.ok() || batch == nullptr) {
      break;
    }
    result.row_count += batch->num_rows();
    UpdateMd5FromBatch(&md5, batch);
  }
  result.md5 = md5.Final();
  return result;
}

}  // namespace

TEST(CustomerGeneratorTest, ScaleFactor001Md5) {
  benchgen::GeneratorOptions options;
  options.scale_factor = 0.01;
  options.chunk_size = 128;
  // Distributions are embedded in the binary.

  std::unique_ptr<benchgen::RecordBatchIterator> iterator;
  auto status = benchgen::MakeRecordBatchIterator(
      benchgen::SuiteId::kTpcds, "customer", options, &iterator);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto result = ComputeCustomerMd5(iterator.get());
  EXPECT_EQ(result.md5, "d7fbf74d3a6902abc28fd90d2cf6e0d9");
  EXPECT_EQ(result.row_count, 1000);
}

TEST(CustomerGeneratorTest, ScaleFactor001Md5DifferentChunk) {
  benchgen::GeneratorOptions options;
  options.scale_factor = 0.01;
  options.chunk_size = 17;
  // Distributions are embedded in the binary.

  std::unique_ptr<benchgen::RecordBatchIterator> iterator;
  auto status = benchgen::MakeRecordBatchIterator(
      benchgen::SuiteId::kTpcds, "customer", options, &iterator);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto result = ComputeCustomerMd5(iterator.get());
  EXPECT_EQ(result.md5, "d7fbf74d3a6902abc28fd90d2cf6e0d9");
  EXPECT_EQ(result.row_count, 1000);
}
