# benchmark-gen

Unified C++17 generators for TPC-H, TPC-DS, and SSB. The C++ API streams Arrow
`RecordBatch` data, while the `benchgen` CLI emits the canonical pipe-delimited
`.tbl`/`.dat` outputs. Select the suite with `--benchmark`.

## Build

### Prerequisites
- CMake 3.16+
- C++17 compiler
- Apache Arrow C++ 19.0.1+
- Python 3 (required to embed distribution files during the build)
- GoogleTest 1.10.0+ (only if tests are enabled)

### Quick Build (System Dependencies)
```sh
cmake -S . -B build
cmake --build build
```

### Build and Run Tests
```sh
cmake -S . -B build -DBENCHGEN_ENABLE_TESTS=ON
cmake --build build
ctest --test-dir build
```

### Custom Arrow/GTest Prefixes
If Arrow or GTest are found under the prefixes, they are used; otherwise the
build falls back to fetching them.
```sh
cmake -S . -B build \
  -DBENCHGEN_ARROW_PREFIX=/path/to/arrow \
  -DBENCHGEN_ENABLE_TESTS=ON \
  -DBENCHGEN_GTEST_PREFIX=/path/to/gtest
cmake --build build
```

### Common CMake Options
- `-DBENCHGEN_STATIC_STDLIB=ON`: statically link the C++ standard library.

### Install to a Custom Directory
```sh
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/path/to/install
cmake --build build
cmake --build build --target install
```

## CLI Usage

### Common `benchgen` options
- `--benchmark`, `-b`: `tpch`, `tpcds`, or `ssb`
- `--table`, `-t`: table name (see suite-specific lists in `include/benchgen/table.h`)
- `--scale`, `--scale-factor`, `-s`: scale factor (default: 1)
- `--chunk-size`: rows per `RecordBatch` (default: 10000)
- `--start-row`: 0-based row offset (default: 0)
- `--row-count`: number of rows to emit (default: -1 = to end)
- `--output`, `-o`: output path (required for TPC-DS; optional for others)
- `--dbgen-seed-mode`: TPCH/SSB seed init (`per-table` default; `all-tables` matches `dbgen -T a`)
- `--parallel`: worker thread count (default: 1; requires `--output` when parallel generation applies, emits `--output`-prefixed parts, and falls back to serial if total rows unknown)

### TPC-H example
```sh
./build/src/benchgen --benchmark tpch \
  --table customer \
  --scale 1 \
  --output build/customer.tbl \
  --chunk-size 10000
```

### TPC-DS example
```sh
./build/src/benchgen --benchmark tpcds \
  --table customer \
  --scale 1 \
  --output build/customer.dat \
  --chunk-size 10000
```

### SSB example
```sh
./build/src/benchgen --benchmark ssb \
  --table customer \
  --scale-factor 1 \
  --output build/customer.tbl
```

Use `--dbgen-seed-mode all-tables` to match `dbgen -T a` output.

### Row-range example (partitionable output)
```sh
./build/src/benchgen --benchmark tpch \
  --table lineitem \
  --scale 10 \
  --start-row 500000 \
  --row-count 200000 \
  --output build/lineitem_500k.tbl
```

### Generate Arrow schema
```sh
./build/src/gen_schema --benchmark tpcds \
  --output build/tpcds_schema.json
```
Use `--benchmark tpch` or `--benchmark ssb` to generate schemas for other suites.
`gen_schema` also accepts `--scale`/`--scale-factor` and `--dbgen-seed-mode`.

## C++ API
Public headers live in `include/benchgen`. The core entry points are
`MakeBenchmarkSuite` and `MakeRecordBatchIterator`, driven by
`GeneratorOptions` (scale, row ranges, chunk size, column projection, and SSB
seed mode).

```c++
#include "benchgen/generator_options.h"
#include "benchgen/record_batch_iterator_factory.h"

benchgen::GeneratorOptions options;
options.scale_factor = 1;
options.start_row = 0;
options.row_count = 100000;
options.column_names = {"c_custkey", "c_name"};

std::unique_ptr<benchgen::RecordBatchIterator> iter;
auto status = benchgen::MakeRecordBatchIterator(
    benchgen::SuiteId::kTpch, "customer", options, &iter);
if (!status.ok()) {
  // Handle error.
}
```

## Project Layout
- `include/benchgen/`: public API headers (suite interfaces, generator options)
- `src/tpch/`, `src/tpcds/`, `src/ssb/`: benchmark implementations
- `src/common/`: shared CLI entrypoints
- `src/util/`: shared utilities
- `resources/<benchmark>/distribution/`: distribution files
- `scripts/`: helper scripts (`scripts/<benchmark>/` for benchmark-specific helpers)
- `tests/<benchmark>/`: gtest-based verification

## Notes
- Distributions are embedded into binaries at build time from
  `resources/<benchmark>/distribution`.
- Deterministic output allows chunked workflows via disjoint
  `--start-row`/`--row-count` ranges. `--parallel` splits the resolved row range
  across worker threads when total row counts are known, writing `-0`, `-1`,
  ... part files based on the `--output` prefix. For tables with unknown total
  row counts (TPC-H `lineitem`, SSB `lineorder`), `benchgen` runs serially even
  if `--parallel > 1`.
- Column projection is supported in the C++ API via
  `GeneratorOptions::column_names`.

## Legal Notice
See `NOTICE`.
