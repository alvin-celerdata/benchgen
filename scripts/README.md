# Scripts: Generate and Validate Output

This folder contains helper scripts for generating and validating benchmark
outputs with `benchgen`. The scripts default to scale 10 and a representative
subset of tables, but can be customized via environment variables.

## Generate (Parallel Parts)

Generate tables in parallel. When `--parallel` is used, `benchgen` writes part
files named with `-0`, `-1`, ... suffixes before the file extension.

```sh
TPCH_TABLES="customer orders" TPCDS_TABLES="customer" SSB_TABLES="customer part" \
TPCH_PARALLEL_COUNT=8 TPCDS_PARALLEL_COUNT=8 SSB_PARALLEL_COUNT=8 \
OUT_DIR=./build/generated SCALE=10 \
  ./scripts/generate_scale10_parallel.sh
```

Notes:
- `BENCHGEN_BIN` (or legacy `GEN_TABLE_BIN`) can be set to a specific
  `benchgen` path. Otherwise, the script expects it under `build/`.
- Outputs go to `OUT_DIR` (default `build/generated`).

## Validate (Official Generators)

Validate generated outputs against official generators (TPC-H dbgen, TPC-DS
dsdgen, SSB dbgen). The validator compares MD5s of parallel part outputs
against official outputs without merging part files.

```sh
OUT_DIR=./build/generated \
TPCH_DBGEN_BIN=./.tmp/tpch-dbgen/dbgen \
TPCDS_DSDGEN_BIN=./.tmp/tpcds-kit/tools/dsdgen \
SSB_DBGEN_BIN=./.tmp/ssb-dbgen/dbgen \
TPCH_TABLES="customer orders" TPCDS_TABLES="customer" SSB_TABLES="customer part" \
  ./scripts/validate_scale10_parallel.sh
```

Notes:
- Requires `md5sum` on PATH.
- Official outputs are written under `OFFICIAL_OUT_DIR` (default
  `build/official_generated`).
- Missing outputs or gaps in part indices are treated as errors.

## Validate (Parallel vs Serial)

Compare parallel outputs against serial outputs produced by `benchgen` with
the same scale factor and seed mode.

```sh
OUT_DIR=./build/generated \
SERIAL_OUT_DIR=./build/generated_serial \
TPCH_TABLES="customer orders" TPCDS_TABLES="customer" SSB_TABLES="customer part" \
  ./scripts/validate_scale10_parallel_vs_serial.sh
```

Notes:
- Requires `benchgen` under `build/` or `BENCHGEN_BIN` (legacy `GEN_TABLE_BIN`)
  to be set.
- Serial outputs are written under `SERIAL_OUT_DIR` (default
  `build/generated_serial`).
