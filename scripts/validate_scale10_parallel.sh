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
OFFICIAL_OUT_DIR="${OFFICIAL_OUT_DIR:-${BUILD_DIR}/official_generated}"
SCALE="${SCALE:-10}"

TPCH_TABLES_STR="${TPCH_TABLES:-customer}"
TPCDS_TABLES_STR="${TPCDS_TABLES:-customer}"
SSB_TABLES_STR="${SSB_TABLES:-customer part supplier date lineorder}"

TPCH_DBGEN_BIN="${TPCH_DBGEN_BIN:-${ROOT_DIR}/.tmp/tpch-dbgen/dbgen}"
TPCDS_DSDGEN_BIN="${TPCDS_DSDGEN_BIN:-${ROOT_DIR}/.tmp/tpcds-kit/tools/dsdgen}"
SSB_DBGEN_BIN="${SSB_DBGEN_BIN:-${ROOT_DIR}/.tmp/ssb-dbgen/dbgen}"
TPCDS_DSDGEN_DISTS="${TPCDS_DSDGEN_DISTS:-}"
TPCH_DBGEN_CONFIG_DIR="${TPCH_DBGEN_CONFIG_DIR:-}"
SSB_DBGEN_CONFIG_DIR="${SSB_DBGEN_CONFIG_DIR:-}"

read -r -a TPCH_TABLES <<< "${TPCH_TABLES_STR}"
read -r -a TPCDS_TABLES <<< "${TPCDS_TABLES_STR}"
read -r -a SSB_TABLES <<< "${SSB_TABLES_STR}"

if ! command -v md5sum >/dev/null 2>&1; then
  echo "md5sum is required on PATH" >&2
  exit 1
fi

shopt -s nullglob

function require_executable() {
  local label="$1"
  local path="$2"
  if [[ -z "${path}" ]]; then
    echo "${label} is not set" >&2
    return 1
  fi
  if [[ ! -x "${path}" ]]; then
    echo "${label} is not executable: ${path}" >&2
    return 1
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

function tpch_table_code() {
  local table="$1"
  case "${table}" in
    customer) echo "c" ;;
    supplier) echo "s" ;;
    nation) echo "n" ;;
    region) echo "r" ;;
    part) echo "P" ;;
    partsupp) echo "S" ;;
    orders) echo "O" ;;
    lineitem) echo "L" ;;
    *) return 1 ;;
  esac
}

function ssb_table_code() {
  local table="$1"
  case "${table}" in
    customer) echo "c" ;;
    part) echo "p" ;;
    supplier) echo "s" ;;
    date) echo "d" ;;
    lineorder) echo "l" ;;
    *) return 1 ;;
  esac
}

function generate_tpch_official_table() {
  local table="$1"
  local output_dir="$2"
  local code
  if ! code="$(tpch_table_code "${table}")"; then
    echo "Unknown TPC-H table: ${table}" >&2
    return 1
  fi
  local config_dir
  config_dir="${TPCH_DBGEN_CONFIG_DIR:-$(dirname -- "${TPCH_DBGEN_BIN}")}"
  DSS_PATH="${output_dir}" DSS_CONFIG="${config_dir}" \
    "${TPCH_DBGEN_BIN}" -s "${SCALE}" -T "${code}" -f
}

function generate_tpcds_official_table() {
  local table="$1"
  local output_dir="$2"
  local dsdgen_dir
  dsdgen_dir="$(dirname -- "${TPCDS_DSDGEN_BIN}")"
  local dists
  dists="${TPCDS_DSDGEN_DISTS:-${dsdgen_dir}/tpcds.idx}"
  if [[ ! -f "${dists}" ]]; then
    echo "TPC-DS distributions file not found: ${dists}" >&2
    return 1
  fi
  "${TPCDS_DSDGEN_BIN}" -SCALE "${SCALE}" -TABLE "${table}" \
    -DIR "${output_dir}" -DISTRIBUTIONS "${dists}" -FORCE Y
}

function tpcds_parent_table() {
  local table="$1"
  case "${table}" in
    catalog_returns) echo "catalog_sales" ;;
    store_returns) echo "store_sales" ;;
    web_returns) echo "web_sales" ;;
    *) return 1 ;;
  esac
}

function generate_ssb_official_table() {
  local table="$1"
  local output_dir="$2"
  local code
  if ! code="$(ssb_table_code "${table}")"; then
    echo "Unknown SSB table: ${table}" >&2
    return 1
  fi
  local config_dir
  config_dir="${SSB_DBGEN_CONFIG_DIR:-$(dirname -- "${SSB_DBGEN_BIN}")}"
  DSS_PATH="${output_dir}" DSS_CONFIG="${config_dir}" \
    "${SSB_DBGEN_BIN}" -s "${SCALE}" -T "${code}" -f
}

function compare_hashes() {
  local suite="$1"
  local table="$2"
  local output="$3"
  local official="$4"
  local parallel_md5
  parallel_md5="$(md5_parallel_output "${output}")"
  local official_md5
  official_md5="$(md5_file "${official}")"
  if [[ "${parallel_md5}" == "${official_md5}" ]]; then
    echo "OK ${suite}/${table} ${parallel_md5}"
    return 0
  fi
  echo "MISMATCH ${suite}/${table} parallel=${parallel_md5} official=${official_md5}" >&2
  return 1
}

function validate_tpch() {
  if [[ "${#TPCH_TABLES[@]}" -eq 0 ]]; then
    return 0
  fi
  require_executable "TPCH_DBGEN_BIN" "${TPCH_DBGEN_BIN}"
  local out_dir="${OUT_DIR}/tpch"
  local official_dir="${OFFICIAL_OUT_DIR}/tpch"
  mkdir -p "${official_dir}"
  local status=0
  for table in "${TPCH_TABLES[@]}"; do
    generate_tpch_official_table "${table}" "${official_dir}"
    if ! compare_hashes "tpch" "${table}" "${out_dir}/${table}.tbl" \
      "${official_dir}/${table}.tbl"; then
      status=1
    fi
  done
  return "${status}"
}

function validate_tpcds() {
  if [[ "${#TPCDS_TABLES[@]}" -eq 0 ]]; then
    return 0
  fi
  require_executable "TPCDS_DSDGEN_BIN" "${TPCDS_DSDGEN_BIN}"
  local out_dir="${OUT_DIR}/tpcds"
  local official_dir="${OFFICIAL_OUT_DIR}/tpcds"
  mkdir -p "${official_dir}"
  local generated_tables=" "
  local status=0
  for table in "${TPCDS_TABLES[@]}"; do
    local generator_table="${table}"
    local parent_table
    if parent_table="$(tpcds_parent_table "${table}")"; then
      generator_table="${parent_table}"
    fi
    if [[ "${generated_tables}" != *" ${generator_table} "* ]]; then
      generate_tpcds_official_table "${generator_table}" "${official_dir}"
      generated_tables="${generated_tables}${generator_table} "
    fi
    if ! compare_hashes "tpcds" "${table}" "${out_dir}/${table}.dat" \
      "${official_dir}/${table}.dat"; then
      status=1
    fi
  done
  return "${status}"
}

function validate_ssb() {
  if [[ "${#SSB_TABLES[@]}" -eq 0 ]]; then
    return 0
  fi
  require_executable "SSB_DBGEN_BIN" "${SSB_DBGEN_BIN}"
  local out_dir="${OUT_DIR}/ssb"
  local official_dir="${OFFICIAL_OUT_DIR}/ssb"
  mkdir -p "${official_dir}"
  local status=0
  for table in "${SSB_TABLES[@]}"; do
    generate_ssb_official_table "${table}" "${official_dir}"
    if ! compare_hashes "ssb" "${table}" "${out_dir}/${table}.tbl" \
      "${official_dir}/${table}.tbl"; then
      status=1
    fi
  done
  return "${status}"
}

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
