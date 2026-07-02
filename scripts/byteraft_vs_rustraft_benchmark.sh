#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: byteraft_vs_rustraft_benchmark.sh [--rustraft-root PATH] [--byteraft-root PATH] [--out PATH] [--release]

Runs the standalone RustRaft ByteRaft parity benchmark harness from outside
TemporalStore and writes the JSON report to --out.

Environment:
  RUSTRAFT_ROOT   RustRaft checkout. Defaults to this script's parent repo.
  BYTERAFT_ROOT   Optional ByteRaft checkout path recorded for runner context.
  BENCHMARK_OUT   Output report path.

The current harness uses RustRaft's same-machine ByteRaft model runner. A real
ByteRaft executable can be wired later behind the same script without changing
TemporalStore.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rustraft_root="${RUSTRAFT_ROOT:-$(cd -- "$script_dir/.." && pwd)}"
byteraft_root="${BYTERAFT_ROOT:-}"
out_path="${BENCHMARK_OUT:-$rustraft_root/target/byteraft-vs-rustraft-benchmark/report.json}"
cargo_profile=()

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
    --out)
      out_path="$2"
      shift 2
      ;;
    --release)
      cargo_profile=(--release)
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

if [[ -n "$byteraft_root" && ! -d "$byteraft_root" ]]; then
  echo "ByteRaft root does not exist: $byteraft_root" >&2
  exit 2
fi

mkdir -p "$(dirname -- "$out_path")"

tmp_report="$(mktemp)"
trap 'rm -f "$tmp_report"' EXIT

cargo run \
  --manifest-path "$rustraft_root/Cargo.toml" \
  "${cargo_profile[@]}" \
  --example byteraft_parity_benchmark \
  >"$tmp_report"

mv "$tmp_report" "$out_path"

echo "ByteRaft-vs-RustRaft benchmark report: $out_path"
if [[ -n "$byteraft_root" ]]; then
  echo "ByteRaft root: $byteraft_root"
fi
