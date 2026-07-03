#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: byteraft_vs_rustraft_benchmark.sh [--rustraft-root PATH] [--byteraft-root PATH] [--byteraft-bin PATH] [--out PATH] [--release] [--native-kvbench-adapter]

Runs the standalone RustRaft ByteRaft parity benchmark harness from outside
TemporalStore and writes the JSON report to --out.

Environment:
  RUSTRAFT_ROOT   RustRaft checkout. Defaults to this script's parent repo.
  BYTERAFT_ROOT   ByteRaft checkout path. Defaults to RustRaft thirdparty/byteraft.
  BYTERAFT_BENCHMARK_BIN  Real ByteRaft benchmark harness executable.
  BYTERAFT_USE_NATIVE_KVBENCH_ADAPTER=1
                  Use RustRaft's native ByteRaft kvbench adapter when the full
                  byteraft_parity_benchmark harness is absent.
  BENCHMARK_OUT   Output report path.

Production parity is fail-closed: the script requires a real ByteRaft harness
and never falls back to the model runner. If the checkout exposes a
byteraft_parity_benchmark build hook or CMake/Bazel target, the script builds it
before failing closed. A native ByteRaft kvbench checkout is reported as partial
evidence only; it is not accepted as full production parity.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rustraft_root="${RUSTRAFT_ROOT:-$(cd -- "$script_dir/.." && pwd)}"
byteraft_root="${BYTERAFT_ROOT:-$rustraft_root/thirdparty/byteraft}"
byteraft_bin="${BYTERAFT_BENCHMARK_BIN:-}"
out_path="${BENCHMARK_OUT:-$rustraft_root/target/byteraft-vs-rustraft-benchmark/report.json}"
cargo_profile=()
build_profile=debug
use_native_kvbench_adapter="${BYTERAFT_USE_NATIVE_KVBENCH_ADAPTER:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rustraft-root)
      rustraft_root="$2"
      shift 2
      ;;
    --byteraft-root)
      byteraft_root="$2"
      shift 2
      ;;
    --byteraft-bin)
      byteraft_bin="$2"
      shift 2
      ;;
    --out)
      out_path="$2"
      shift 2
      ;;
    --release)
      cargo_profile=(--release)
      build_profile=release
      shift
      ;;
    --native-kvbench-adapter)
      use_native_kvbench_adapter=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$rustraft_root/Cargo.toml" ]]; then
  echo "RustRaft root is missing Cargo.toml: $rustraft_root" >&2
  exit 2
fi

find_byteraft_bin() {
  for candidate in \
    "$byteraft_root/target/release/byteraft_parity_benchmark" \
    "$byteraft_root/target/debug/byteraft_parity_benchmark" \
    "$byteraft_root/build/byteraft_parity_benchmark" \
    "$byteraft_root/bin/byteraft_parity_benchmark" \
    "$byteraft_root/byteraft_parity_benchmark"; do
    if [[ -x "$candidate" || -f "$candidate" ]]; then
      byteraft_bin="$candidate"
      return 0
    fi
  done
  return 1
}

try_build_byteraft_harness() {
  if [[ ! -d "$byteraft_root" ]]; then
    return 0
  fi

  for build_script in \
    "$byteraft_root/scripts/build_byteraft_parity_benchmark.sh" \
    "$byteraft_root/build_byteraft_parity_benchmark.sh"; do
    if [[ -f "$build_script" ]]; then
      bash "$build_script" --profile "$build_profile" || return 1
      find_byteraft_bin && return 0
    fi
  done

  if [[ -f "$byteraft_root/CMakeLists.txt" ]] && grep -q "byteraft_parity_benchmark" "$byteraft_root/CMakeLists.txt"; then
    cmake_build_type=Debug
    if [[ "$build_profile" == "release" ]]; then
      cmake_build_type=Release
    fi
    cmake -S "$byteraft_root" -B "$byteraft_root/build" -DCMAKE_BUILD_TYPE="$cmake_build_type"
    cmake --build "$byteraft_root/build" --target byteraft_parity_benchmark
    find_byteraft_bin && return 0
  fi

  if [[ -f "$byteraft_root/BUILD" ]] && grep -q "byteraft_parity_benchmark" "$byteraft_root/BUILD"; then
    (cd "$byteraft_root" && bazel build //:byteraft_parity_benchmark)
    mkdir -p "$byteraft_root/bin"
    if [[ -f "$byteraft_root/bazel-bin/byteraft_parity_benchmark" ]]; then
      cp "$byteraft_root/bazel-bin/byteraft_parity_benchmark" "$byteraft_root/bin/byteraft_parity_benchmark"
    fi
    find_byteraft_bin && return 0
  fi

  return 0
}

native_byteraft_capability_report() {
  if [[ ! -d "$byteraft_root" ]]; then
    echo "ByteRaft native capability: root missing ($byteraft_root)" >&2
    return 0
  fi

  local kvbench=""
  for candidate in \
    "$byteraft_root/build/example/kv/kvbench" \
    "$byteraft_root/build/example/kv/kv_benchmark" \
    "$byteraft_root/bin/kvbench" \
    "$byteraft_root/kvbench"; do
    if [[ -f "$candidate" ]]; then
      kvbench="$candidate"
      break
    fi
  done

  local source="missing"
  local script="missing"
  local cmake_target="missing"
  [[ -f "$byteraft_root/example/kv/kv_benchmark.cc" ]] && source="$byteraft_root/example/kv/kv_benchmark.cc"
  [[ -f "$byteraft_root/script/bench.sh" ]] && script="$byteraft_root/script/bench.sh"
  if [[ -f "$byteraft_root/example/kv/CMakeLists.txt" ]] && grep -q "add_executable(kvbench" "$byteraft_root/example/kv/CMakeLists.txt"; then
    cmake_target="present"
  fi

  echo "ByteRaft native capability: kvbench=${kvbench:-missing} source=$source script=$script cmake_kvbench_target=$cmake_target" >&2
  echo "ByteRaft native capability is partial: it can inform single-key client write/read benchmarking, but the full parity harness must still cover batched writes, replication batching, WAL fsync, read-index, lease-read, snapshot install/catch-up, snapshot streaming, and leader transfer under load." >&2
}

if [[ -z "$byteraft_bin" ]]; then
  find_byteraft_bin || true
fi

if [[ ! -d "$byteraft_root" && -z "$byteraft_bin" ]]; then
  echo "benchmark:real_byteraft_missing: ByteRaft root does not exist: $byteraft_root" >&2
  exit 2
fi

if [[ -z "$byteraft_bin" ]]; then
  try_build_byteraft_harness || {
    echo "benchmark:real_byteraft_missing: failed to build ByteRaft benchmark harness under $byteraft_root" >&2
    exit 2
  }
fi

if [[ -z "$byteraft_bin" && "$use_native_kvbench_adapter" == "1" ]]; then
  adapter="$rustraft_root/scripts/byteraft_native_kvbench_adapter.sh"
  if [[ -f "$adapter" ]]; then
    native_byteraft_capability_report
    echo "ByteRaft native kvbench adapter enabled: $adapter" >&2
    echo "Production parity is still expected to fail until unsupported workloads are covered by a full ByteRaft harness." >&2
    byteraft_bin="$adapter"
  fi
fi

if [[ -z "$byteraft_bin" || ! -f "$byteraft_bin" ]]; then
  native_byteraft_capability_report
  echo "benchmark:real_byteraft_missing: no ByteRaft benchmark harness found under $byteraft_root; set BYTERAFT_BENCHMARK_BIN" >&2
  exit 2
fi

mkdir -p "$(dirname -- "$out_path")"

tmp_report="$(mktemp)"
trap 'rm -f "$tmp_report"' EXIT

set +e
BYTERAFT_ROOT="$byteraft_root" \
BYTERAFT_BENCHMARK_BIN="$byteraft_bin" \
RUSTRAFT_BENCHMARK_PROFILE="$build_profile" \
cargo run \
  --manifest-path "$rustraft_root/Cargo.toml" \
  "${cargo_profile[@]}" \
  --example byteraft_parity_benchmark \
  >"$tmp_report"
benchmark_status=$?
set -e

if [[ -s "$tmp_report" ]]; then
  mv "$tmp_report" "$out_path"
else
  rm -f "$tmp_report"
fi

echo "ByteRaft-vs-RustRaft benchmark report: $out_path"
echo "ByteRaft root: $byteraft_root"
echo "ByteRaft benchmark harness: $byteraft_bin"
exit "$benchmark_status"
