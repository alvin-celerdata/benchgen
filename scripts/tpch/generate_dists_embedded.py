#!/usr/bin/env python3
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

"""
Generate a C++ source file that embeds dists.dss into a raw string literal.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


def choose_delimiter(data: str, base: str) -> str:
    delimiter = base
    if f"){delimiter}" not in data:
        return delimiter
    index = 1
    while True:
        delimiter = f"{base}_{index}"
        if f"){delimiter}" not in data:
            return delimiter
        index += 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Embed dists.dss into a generated C++ source file."
    )
    parser.add_argument("--input", required=True, help="Path to dists.dss")
    parser.add_argument("--output", required=True, help="Path to output .cc file")
    parser.add_argument(
        "--label",
        help="Optional label for the header comment (defaults to input path)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    try:
        data = input_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        data = input_path.read_bytes().decode("latin-1")

    data = data.replace("\r\n", "\n").replace("\r", "\n")
    if not data.endswith("\n"):
        data += "\n"

    delimiter = choose_delimiter(data, "TPCH_DISTS")
    label = args.label if args.label else str(args.input)

    output = (
        f"/* Auto-generated from {label}. */\n"
        "#include <cstddef>\n\n"
        "extern \"C\" {\n"
        f"extern const char tpch_dists_data[] = R\"{delimiter}(\n"
        f"{data}"
        f"){delimiter}\";\n"
        "extern const size_t tpch_dists_size = sizeof(tpch_dists_data) - 1;\n"
        "}  // extern \"C\"\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output, encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
