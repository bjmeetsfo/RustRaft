#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: byteraft_vs_rustraft_benchmark.sh [--rustraft-root PATH] [--byteraft-root PATH] [--byteraft-bin PATH] [--out PATH] [--release]

Runs the standalone RustRaft ByteRaft parity benchmark harness from outside
TemporalStore and writes the JSON report to --out.

Environment:
  RUSTRAFT_ROOT   RustRaft checkout. Defaults to this script's parent repo.
  BYTERAFT_ROOT   ByteRaft checkout path. Defaults to RustRaft thirdparty/byteraft.
  BYTERAFT_BENCHMARK_BIN  Real ByteRaft benchmark harness executable.
  BENCHMARK_OUT   Output report path.

Production parity is fail-closed: the script requires a real ByteRaft harness
and never falls back to the model runner.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rustraft_root="${RUSTRAFT_ROOT:-$(cd -- "$script_dir/.." && pwd)}"
byteraft_root="${BYTERAFT_ROOT:-$rustraft_root/thirdparty/byteraft}"
byteraft_bin="${BYTERAFT_BENCHMARK_BIN:-}"
out_path="${BENCHMARK_OUT:-$rustraft_root/target/byteraft-vs-rustraft-benchmark/report.json}"
cargo_profile=()
build_profile=debug

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

if [[ -z "$byteraft_bin" ]]; then
  for candidate in \
    "$byteraft_root/target/release/byteraft_parity_benchmark" \
    "$byteraft_root/target/debug/byteraft_parity_benchmark" \
    "$byteraft_root/build/byteraft_parity_benchmark" \
    "$byteraft_root/bin/byteraft_parity_benchmark" \
    "$byteraft_root/byteraft_parity_benchmark"; do
    if [[ -x "$candidate" || -f "$candidate" ]]; then
      byteraft_bin="$candidate"
      break
    fi
  done
fi

if [[ ! -d "$byteraft_root" && -z "$byteraft_bin" ]]; then
  echo "benchmark:real_byteraft_missing: ByteRaft root does not exist: $byteraft_root" >&2
  exit 2
fi

if [[ -z "$byteraft_bin" || ! -f "$byteraft_bin" ]]; then
  echo "benchmark:real_byteraft_missing: no ByteRaft benchmark harness found under $byteraft_root; set BYTERAFT_BENCHMARK_BIN" >&2
  exit 2
fi

mkdir -p "$(dirname -- "$out_path")"

tmp_report="$(mktemp)"
trap 'rm -f "$tmp_report"' EXIT

BYTERAFT_ROOT="$byteraft_root" \
BYTERAFT_BENCHMARK_BIN="$byteraft_bin" \
RUSTRAFT_BENCHMARK_PROFILE="$build_profile" \
cargo run \
  --manifest-path "$rustraft_root/Cargo.toml" \
  "${cargo_profile[@]}" \
  --example byteraft_parity_benchmark \
  >"$tmp_report"

mv "$tmp_report" "$out_path"

echo "ByteRaft-vs-RustRaft benchmark report: $out_path"
echo "ByteRaft root: $byteraft_root"
echo "ByteRaft benchmark harness: $byteraft_bin"
