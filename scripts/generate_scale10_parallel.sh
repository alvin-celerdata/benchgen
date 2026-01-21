#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
OUT_DIR="${OUT_DIR:-${BUILD_DIR}/generated}"
SCALE="${SCALE:-10}"

TPCH_TABLES_STR="${TPCH_TABLES:-customer}"
TPCDS_TABLES_STR="${TPCDS_TABLES:-customer}"
SSB_TABLES_STR="${SSB_TABLES:-customer part supplier date lineorder}"
TPCH_PARALLEL_COUNT="${TPCH_PARALLEL_COUNT:-4}"
TPCDS_PARALLEL_COUNT="${TPCDS_PARALLEL_COUNT:-4}"
SSB_PARALLEL_COUNT="${SSB_PARALLEL_COUNT:-4}"

read -r -a TPCH_TABLES <<< "${TPCH_TABLES_STR}"
read -r -a TPCDS_TABLES <<< "${TPCDS_TABLES_STR}"
read -r -a SSB_TABLES <<< "${SSB_TABLES_STR}"

function resolve_benchgen() {
  local candidate
  for candidate in "${BUILD_DIR}/src/benchgen" "${BUILD_DIR}/benchgen" \
    "${BUILD_DIR}/src/gen_table" "${BUILD_DIR}/gen_table"; do
    if [[ -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

function require_benchgen() {
  if [[ -n "${BENCHGEN_BIN:-}" ]]; then
    if [[ ! -x "${BENCHGEN_BIN}" ]]; then
      echo "BENCHGEN_BIN is not executable: ${BENCHGEN_BIN}" >&2
      exit 1
    fi
    return 0
  fi
  if [[ -n "${GEN_TABLE_BIN:-}" ]]; then
    if [[ ! -x "${GEN_TABLE_BIN}" ]]; then
      echo "GEN_TABLE_BIN is not executable: ${GEN_TABLE_BIN}" >&2
      exit 1
    fi
    BENCHGEN_BIN="${GEN_TABLE_BIN}"
    return 0
  fi
  if ! BENCHGEN_BIN="$(resolve_benchgen)"; then
    echo "benchgen binary not found under ${BUILD_DIR}" >&2
    echo "Build it with: cmake -S ${ROOT_DIR} -B ${BUILD_DIR} && cmake --build ${BUILD_DIR}" >&2
    exit 1
  fi
}

function generate_tpch() {
  local out_dir="${OUT_DIR}/tpch"
  mkdir -p "${out_dir}"

  for table in "${TPCH_TABLES[@]}"; do
    local output="${out_dir}/${table}.tbl"
    if [[ "${TPCH_PARALLEL_COUNT}" -gt 1 ]]; then
      "${BENCHGEN_BIN}" --benchmark tpch --table "${table}" \
        --scale "${SCALE}" --parallel "${TPCH_PARALLEL_COUNT}" \
        --output "${output}"
    else
      "${BENCHGEN_BIN}" --benchmark tpch --table "${table}" \
        --scale "${SCALE}" --output "${output}"
    fi
  done
}

function generate_tpcds() {
  local out_dir="${OUT_DIR}/tpcds"
  mkdir -p "${out_dir}"

  for table in "${TPCDS_TABLES[@]}"; do
    local output="${out_dir}/${table}.dat"
    if [[ "${TPCDS_PARALLEL_COUNT}" -gt 1 ]]; then
      "${BENCHGEN_BIN}" --benchmark tpcds --table "${table}" \
        --scale "${SCALE}" --parallel "${TPCDS_PARALLEL_COUNT}" \
        --output "${output}"
    else
      "${BENCHGEN_BIN}" --benchmark tpcds --table "${table}" \
        --scale "${SCALE}" --output "${output}"
    fi
  done
}

function generate_ssb() {
  local out_dir="${OUT_DIR}/ssb"
  mkdir -p "${out_dir}"

  for table in "${SSB_TABLES[@]}"; do
    local output="${out_dir}/${table}.tbl"
    if [[ "${SSB_PARALLEL_COUNT}" -gt 1 ]]; then
      "${BENCHGEN_BIN}" --benchmark ssb --table "${table}" \
        --scale-factor "${SCALE}" --dbgen-seed-mode per-table \
        --parallel "${SSB_PARALLEL_COUNT}" --output "${output}"
    else
      "${BENCHGEN_BIN}" --benchmark ssb --table "${table}" \
        --scale-factor "${SCALE}" --dbgen-seed-mode per-table \
        --output "${output}"
    fi
  done
}

require_benchgen
generate_tpch
generate_tpcds
generate_ssb
