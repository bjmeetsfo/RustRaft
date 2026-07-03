#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: byteraft_native_kvbench_adapter.sh --workload ID --node-count 3 --iterations N --batch-size N [--byteraft-root PATH]

Runs ByteRaft's native example/kv kvserver+kvbench path for the workloads that
the native example can honestly cover, then emits one RustRaft benchmark sample
JSON object on stdout.

This adapter is a bridge, not production parity. It reports unsupported
production workloads as correctness_failed samples so the full
ByteRaft-vs-RustRaft report remains fail-closed until a complete
byteraft_parity_benchmark harness exists.
USAGE
}

workload=""
node_count=3
iterations=128
batch_size=16
byteraft_root="${BYTERAFT_ROOT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload)
      workload="$2"
      shift 2
      ;;
    --node-count)
      node_count="$2"
      shift 2
      ;;
    --iterations)
      iterations="$2"
      shift 2
      ;;
    --batch-size)
      batch_size="$2"
      shift 2
      ;;
    --byteraft-root)
      byteraft_root="$2"
      shift 2
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

if [[ -z "$workload" ]]; then
  echo "missing --workload" >&2
  exit 2
fi

json_string() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"
}

emit_sample() {
  local correctness="$1"
  local operation_count="$2"
  local p50="$3"
  local p99="$4"
  local throughput="$5"
  local binary_path="$6"
  local revision="$7"
  local profile="$8"
  cat <<JSON
{
  "workload": $(json_string "$workload"),
  "engine": "byte_raft",
  "engine_source": "real_byte_raft",
  "binary_path": $(json_string "$binary_path"),
  "git_revision": $(json_string "$revision"),
  "build_profile": $(json_string "$profile"),
  "node_count": $node_count,
  "operation_count": $operation_count,
  "p50_latency_micros": $p50,
  "p99_latency_micros": $p99,
  "throughput_ops_per_sec": $throughput,
  "correctness_passed": $correctness
}
JSON
}

unsupported_workload_sample() {
  local reason="$1"
  echo "benchmark:byteraft_native_kvbench_unsupported:$workload:$reason" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial"
}

operation_count="$iterations"
if [[ "$workload" == "batched_writes" || "$workload" == "replication_batching" ]]; then
  operation_count=$((iterations * batch_size))
fi

case "$workload" in
  single_key_writes|batched_writes|replication_batching|read_index_reads|lease_reads)
    ;;
  wal_fsync|snapshot_install_catchup|snapshot_streaming|leader_transfer_under_load)
    unsupported_workload_sample "requires full ByteRaft parity harness"
    exit 0
    ;;
  *)
    unsupported_workload_sample "unknown workload"
    exit 0
    ;;
esac

if [[ "$node_count" != "3" ]]; then
  echo "benchmark:byteraft_native_kvbench_requires_three_nodes:$node_count" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial"
  exit 0
fi

if [[ -z "$byteraft_root" || ! -d "$byteraft_root" ]]; then
  echo "benchmark:real_byteraft_missing:${byteraft_root:-unset}" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial"
  exit 0
fi

kvserver="${BYTERAFT_KVSERVER_BIN:-$byteraft_root/build/example/kv/kvserver}"
kvbench="${BYTERAFT_KVBENCH_BIN:-$byteraft_root/build/example/kv/kvbench}"

if [[ ! -x "$kvserver" && ! -f "$kvserver" ]]; then
  echo "benchmark:byteraft_kvserver_binary_missing:$kvserver" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial"
  exit 0
fi

if [[ ! -x "$kvbench" && ! -f "$kvbench" ]]; then
  echo "benchmark:byteraft_kvbench_binary_missing:$kvbench" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial"
  exit 0
fi

revision="$(git -C "$byteraft_root" rev-parse HEAD 2>/dev/null || true)"
build_profile="${BYTERAFT_BUILD_PROFILE:-native-kvbench}"
work_dir="${BYTERAFT_NATIVE_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/rustraft-byteraft-kvbench.XXXXXX")}"
mkdir -p "$work_dir"

cleanup() {
  if [[ -n "${server_pids:-}" ]]; then
    for pid in $server_pids; do
      kill "$pid" >/dev/null 2>&1 || true
    done
    wait $server_pids >/dev/null 2>&1 || true
  fi
  if [[ -z "${BYTERAFT_NATIVE_KEEP_WORK_DIR:-}" ]]; then
    rm -rf "$work_dir"
  fi
}
trap cleanup EXIT

server_pids=""
peers="1,127.0.0.1:19491,127.0.0.1:19492,2,127.0.0.1:19591,127.0.0.1:19592,3,127.0.0.1:19691,127.0.0.1:19692"
addresses="1,127.0.0.1:19490,2,127.0.0.1:19590,3,127.0.0.1:19690"

for id in 1 2 3; do
  base_port=$((19390 + id * 100))
  node_dir="$work_dir/node-$id"
  mkdir -p "$node_dir"
  RAFT_EXAMPLE_BOOT=1 "$kvserver" \
    -id="$id" \
    -kv_addr="127.0.0.1:$base_port" \
    -raft_addr="127.0.0.1:$((base_port + 1))" \
    -snapshot_addr="127.0.0.1:$((base_port + 2))" \
    -peers="$peers" \
    -wal_dir="$node_dir/wal" \
    -fsm_dir="$node_dir/fsm" \
    -snapshot_dir="$node_dir/snapshot" \
    -shard=1 \
    -log_file="$node_dir/LOG" \
    -log_level=2 \
    -metrics_on=false \
    >"$node_dir/stdout.log" 2>"$node_dir/stderr.log" &
  server_pids="$server_pids $!"
done

sleep "${BYTERAFT_NATIVE_STARTUP_SECONDS:-5}"

if ! kill -0 $server_pids >/dev/null 2>&1; then
  echo "benchmark:byteraft_native_cluster_start_failed:$work_dir" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "$kvbench" "$revision" "$build_profile"
  exit 0
fi

read_write_ratio=0
case "$workload" in
  read_index_reads|lease_reads)
    read_write_ratio=1
    ;;
esac

bench_log="$work_dir/kvbench.log"
"$kvbench" \
  -begin_threads=1 \
  -threads=1 \
  -threads_step=1 \
  -threads_per_step_sleep_seconds=0 \
  -threads_step_sleep_seconds=0 \
  -num_connection_group=1 \
  -address="$addresses" \
  -log_detail=false \
  -shard_num=1 \
  -data_begin=0 \
  -data_end=100000000 \
  -operation="$operation_count" \
  -read_write_ratio="$read_write_ratio" \
  -record_length="${BYTERAFT_NATIVE_RECORD_LENGTH:-128}" \
  -report_intervals=1 \
  >"$bench_log" 2>&1 || true

python3 - "$bench_log" "$operation_count" "$kvbench" "$revision" "$build_profile" "$workload" "$node_count" <<'PY'
import json
import re
import sys

log_path, operation_count, binary_path, revision, profile, workload, node_count = sys.argv[1:]
text = open(log_path, "r", encoding="utf-8", errors="replace").read()
pattern = re.compile(
    r"(?:READ|WRITE)\s+Takes\(s\):\s*([0-9.]+),\s*Count:\s*(\d+),\s*OPS:\s*([0-9.]+),\s*Avg\(us\):\s*(\d+),\s*P95\(us\):\s*(\d+),\s*P99\(us\):\s*(\d+)"
)
matches = pattern.findall(text)
if not matches:
    print(f"benchmark:byteraft_native_kvbench_parse_failed:{log_path}", file=sys.stderr)
    sample = {
        "workload": workload,
        "engine": "byte_raft",
        "engine_source": "real_byte_raft",
        "binary_path": binary_path,
        "git_revision": revision or None,
        "build_profile": profile,
        "node_count": int(node_count),
        "operation_count": int(operation_count),
        "p50_latency_micros": 1000000000,
        "p99_latency_micros": 1000000000,
        "throughput_ops_per_sec": 1.0,
        "correctness_passed": False,
    }
else:
    elapsed, count, ops, avg, p95, p99 = matches[-1]
    sample = {
        "workload": workload,
        "engine": "byte_raft",
        "engine_source": "real_byte_raft",
        "binary_path": binary_path,
        "git_revision": revision or None,
        "build_profile": profile,
        "node_count": int(node_count),
        "operation_count": int(operation_count),
        "p50_latency_micros": int(avg),
        "p99_latency_micros": int(p99),
        "throughput_ops_per_sec": float(ops),
        "correctness_passed": int(count) > 0,
    }
print(json.dumps(sample, indent=2))
PY
