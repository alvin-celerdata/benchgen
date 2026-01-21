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
SERIAL_OUT_DIR="${SERIAL_OUT_DIR:-${BUILD_DIR}/generated_serial}"
SCALE="${SCALE:-10}"

TPCH_TABLES_STR="${TPCH_TABLES:-customer}"
TPCDS_TABLES_STR="${TPCDS_TABLES:-customer}"
SSB_TABLES_STR="${SSB_TABLES:-customer part supplier date lineorder}"
TPCH_SEED_MODE="${TPCH_SEED_MODE:-}"
SSB_SEED_MODE="${SSB_SEED_MODE:-per-table}"

read -r -a TPCH_TABLES <<< "${TPCH_TABLES_STR}"
read -r -a TPCDS_TABLES <<< "${TPCDS_TABLES_STR}"
read -r -a SSB_TABLES <<< "${SSB_TABLES_STR}"

if ! command -v md5sum >/dev/null 2>&1; then
  echo "md5sum is required on PATH" >&2
  exit 1
fi

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

function collect_part_paths() {
  local output="$1"
  local dir
  dir="$(dirname -- "${output}")"
  local base
  base="$(basename -- "${output}")"
  local stem
  local ext
  if [[ "${base}" == *.* ]]; then
    stem="${base%.*}"
    ext=".${base##*.}"
  else
    stem="${base}"
    ext=""
  fi

  local entries=()
  local part
  for part in "${dir}/${stem}-"*${ext}; do
    if [[ ! -e "${part}" ]]; then
      continue
    fi
    local part_base
    part_base="$(basename -- "${part}")"
    local idx
    idx="${part_base#${stem}-}"
    idx="${idx%${ext}}"
    if [[ "${idx}" =~ ^[0-9]+$ ]]; then
      entries+=("${idx}"$'\t'"${part}")
    fi
  done

  if [[ "${#entries[@]}" -eq 0 ]]; then
    return 0
  fi

  local sorted
  sorted="$(printf '%s\n' "${entries[@]}" | sort -t $'\t' -k1,1n)"
  local expected=0
  local paths=()
  local line
  while IFS= read -r line; do
    local idx
    local path
    idx="${line%%$'\t'*}"
    path="${line#*$'\t'}"
    if [[ "${idx}" != "${expected}" ]]; then
      echo "Missing part for ${output}: expected index ${expected}, found ${idx}" >&2
      return 1
    fi
    paths+=("${path}")
    expected=$((expected + 1))
  done <<< "${sorted}"

  printf '%s\n' "${paths[@]}"
}

function md5_parallel_output() {
  local output="$1"
  local parts_output
  parts_output="$(collect_part_paths "${output}")"
  if [[ -n "${parts_output}" ]]; then
    local part_paths=()
    local line
    while IFS= read -r line; do
      part_paths+=("${line}")
    done <<< "${parts_output}"
    cat "${part_paths[@]}" | md5sum | awk '{print $1}'
    return 0
  fi
  if [[ -f "${output}" ]]; then
    md5sum "${output}" | awk '{print $1}'
    return 0
  fi
  echo "Missing output: ${output}" >&2
  return 1
}

function md5_file() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    echo "Missing output: ${path}" >&2
    return 1
  fi
  md5sum "${path}" | awk '{print $1}'
}

function compare_hashes() {
  local suite="$1"
  local table="$2"
  local parallel_output="$3"
  local serial_output="$4"
  local parallel_md5
  parallel_md5="$(md5_parallel_output "${parallel_output}")"
  local serial_md5
  serial_md5="$(md5_file "${serial_output}")"
  if [[ "${parallel_md5}" == "${serial_md5}" ]]; then
    echo "OK ${suite}/${table} ${parallel_md5}"
    return 0
  fi
  echo "MISMATCH ${suite}/${table} parallel=${parallel_md5} serial=${serial_md5}" >&2
  return 1
}

function generate_tpch_serial() {
  local table="$1"
  local output="$2"
  local args=(--benchmark tpch --table "${table}" --scale "${SCALE}" --output "${output}")
  if [[ -n "${TPCH_SEED_MODE}" ]]; then
    args+=(--dbgen-seed-mode "${TPCH_SEED_MODE}")
  fi
  "${BENCHGEN_BIN}" "${args[@]}"
}

function generate_tpcds_serial() {
  local table="$1"
  local output="$2"
  "${BENCHGEN_BIN}" --benchmark tpcds --table "${table}" \
    --scale "${SCALE}" --output "${output}"
}

function generate_ssb_serial() {
  local table="$1"
  local output="$2"
  "${BENCHGEN_BIN}" --benchmark ssb --table "${table}" \
    --scale-factor "${SCALE}" --dbgen-seed-mode "${SSB_SEED_MODE}" \
    --output "${output}"
}

function validate_tpch() {
  if [[ "${#TPCH_TABLES[@]}" -eq 0 ]]; then
    return 0
  fi
  local out_dir="${OUT_DIR}/tpch"
  local serial_dir="${SERIAL_OUT_DIR}/tpch"
  mkdir -p "${serial_dir}"
  local status=0
  for table in "${TPCH_TABLES[@]}"; do
    local serial_output="${serial_dir}/${table}.tbl"
    generate_tpch_serial "${table}" "${serial_output}"
    if ! compare_hashes "tpch" "${table}" "${out_dir}/${table}.tbl" "${serial_output}"; then
      status=1
    fi
  done
  return "${status}"
}

function validate_tpcds() {
  if [[ "${#TPCDS_TABLES[@]}" -eq 0 ]]; then
    return 0
  fi
  local out_dir="${OUT_DIR}/tpcds"
  local serial_dir="${SERIAL_OUT_DIR}/tpcds"
  mkdir -p "${serial_dir}"
  local status=0
  for table in "${TPCDS_TABLES[@]}"; do
    local serial_output="${serial_dir}/${table}.dat"
    generate_tpcds_serial "${table}" "${serial_output}"
    if ! compare_hashes "tpcds" "${table}" "${out_dir}/${table}.dat" "${serial_output}"; then
      status=1
    fi
  done
  return "${status}"
}

function validate_ssb() {
  if [[ "${#SSB_TABLES[@]}" -eq 0 ]]; then
    return 0
  fi
  local out_dir="${OUT_DIR}/ssb"
  local serial_dir="${SERIAL_OUT_DIR}/ssb"
  mkdir -p "${serial_dir}"
  local status=0
  for table in "${SSB_TABLES[@]}"; do
    local serial_output="${serial_dir}/${table}.tbl"
    generate_ssb_serial "${table}" "${serial_output}"
    if ! compare_hashes "ssb" "${table}" "${out_dir}/${table}.tbl" "${serial_output}"; then
      status=1
    fi
  done
  return "${status}"
}

require_benchgen

status=0
if ! validate_tpch; then
  status=1
fi
if ! validate_tpcds; then
  status=1
fi
if ! validate_ssb; then
  status=1
fi

exit "${status}"
