# Repository Guidelines

## Project Structure & Module Organization
- `include/tpch/`, `include/tpcds/`, `include/ssb/`: public API headers (table generators, iterator interfaces, generator options, table metadata).
- `src/tpch/`, `src/tpcds/`, `src/ssb/`: benchmark implementations (row/table generators, RNG/scaling, internal helpers).
- `src/common/`: shared CLI entrypoints.
- `src/util/`: shared utilities (keep light; prefer benchmark-local helpers unless truly shared).
- `resources/tpch/distribution/`, `resources/tpcds/distribution/`, `resources/ssb/distribution/`: distribution files used at runtime.
- `scripts/tpch/`, `scripts/tpcds/`: helper scripts for dependencies and embedded distributions.
- `tests/tpch/`, `tests/tpcds/`, `tests/ssb/`: gtest-based verification.
- `.tmp/`: optional local clones of upstream reference tools; not required for build/runtime.

## Build, Test, and Development Commands
```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build
```
- `cmake -S . -B build`: configure the project (requires Arrow and GTest).
- `cmake --build build`: compile the libraries and CLIs.
- `ctest --test-dir build`: run enabled tests.

Tip: distribution data defaults to `resources/<benchmark>/distribution`. You can override with
`TPCH_DISTRIBUTION_DIR`, `TPCDS_DISTRIBUTION_DIR`, `SSB_DISTRIBUTION_DIR`, or
`GeneratorOptions::distribution_dir` in each API.

## Coding Style & Naming Conventions
- Follow Google C++ style (C++17, 2-space indentation, braces on the same line).
- Types use `CamelCase`, functions/fields use `snake_case`, constants use `kConstant`.
- Namespaces are `tpch`, `tpcds`, and `ssb`; keep public APIs in `include/` and internals in `src/`.

## Testing Guidelines
- Framework: GoogleTest.
- Tests live in `tests/<benchmark>/*_test.cc` and validate deterministic output.
- Run tests via `ctest --test-dir build`.
- If you change RNG, distributions, or column ordering, update expected MD5 values and document the change.

## Commit & Pull Request Guidelines
- No git history is present in this workspace, so no existing commit convention
  can be inferred.
- Suggested: Conventional Commits (`feat:`, `fix:`, `test:`) and small, focused
  commits.
- PRs should describe what changed, which scale factors were tested, and whether
  any checksums or distribution files were updated.

## Configuration & Data Files
- Distribution files are required at runtime; keep their format and encoding intact.
- Avoid introducing new dependencies unless they are required for correctness.
