# RustRaft

RustRaft is the TemporalStore-owned Rust Raft readiness and parity contract
library. It is intentionally small: the crate owns the stable public contract
for RustRaft semantic requirements, readiness evidence, and parity reports,
while TemporalStore owns the storage runtime, data-node integration, and
metaserver integration.

License: Apache-2.0.

## What This Crate Provides

- `RustRaftSemanticRequirement`
- `RustRaftParityContract`
- `RustRaftParityReport`
- `RustRaftProductionReadinessInput`
- `RustRaftProductionReadinessReport`
- `RustRaftProcessRolloutReadinessReport`
- `RustRaftProductionStatus`
- `RustRaftStorage`
- `RustRaftTransport`
- `InMemoryRaftTransport`
- `RustRaftTransportValidationReport`
- `RustRaftStatusSnapshot`
- `RustRaftMetricNames`
- `RustRaftFaultScenario`
- `rustraft_fault_harness_readiness_report`
- `rustraft_read_safety_decision`
- `rustraft_applied_index_fence_report`
- `rustraft_lease_read_eligibility_report`
- `rustraft_bounded_stale_read_report`
- `rustraft_learner_promotion_decision`
- `rustraft_append_safety_decision`
- `RustRaftReadinessEvidence`
- `RustRaftReadinessSnapshot`
- `rustraft_parity_contract`
- `rustraft_parity_report`
- `rustraft_production_readiness_report`
- `rustraft_data_node_process_rollout_readiness_report`
- `rustraft_meta_process_rollout_readiness_report`
- `rustraft_byteraft_runtime_capability_report`
- `rustraft_byteraft_runtime_capability_prometheus`
- `rustraft_public_api_contract`
- `rustraft_open_source_surface`
- `rustraft_temporalstore_adapter_shape`
- `rustraft_temporalstore_extraction_plan`
- `rustraft_metric_names`

The crate is OpenRaft-free and independent of OpenRaft types. TemporalStore
converts its internal readiness evidence into `RustRaftReadinessSnapshot` or
implements `RustRaftReadinessEvidence`, then asks this crate to build a
conservative parity report.

## Production Readiness Status

`rustraft_parity_report` returns both a compatibility boolean and an explicit
production status:

- `blocked`: at least one required safety, durability, transport, snapshot,
  membership, or observability requirement is missing.
- `feature_correct`: the contract shape is usable, but the runtime evidence is
  not enough to claim production readiness.
- `production_ready`: every required semantic is present, OpenRaft is absent
  from the public contract, and the TemporalRaft runtime is available.

Reports include `production_blockers` such as
`durability:storage_apply_fence`, making missing production evidence easy to
surface in TemporalStore readiness gates and CI.

`rustraft_production_readiness_report` is the fail-closed deployment gate. It
wraps the semantic parity report with runtime evidence for peer pipeline,
snapshot lifecycle, WAL lifecycle, data-node rollout, and metaserver rollout.
The data-node and metaserver rollout report helpers expose the same fail-closed
process-path checks independently, so TemporalStore and downstream adopters can
validate spawned-process evidence before composing the full production report.
`rustraft_byteraft_runtime_capability_report` groups the same evidence into
ByteRaft-derived runtime capability families: process-path rollout proof,
per-peer replication pipeline state, reorder queues, snapshot sender/downloader
lifecycle, WAL segment lifecycle, read-index/lease safety, membership role
semantics, FSM apply atomicity, and admin/metrics observability.
`rustraft_byteraft_runtime_capability_prometheus` renders that report as generic
`rustraft_byteraft_*` Prometheus text metrics. Product runtimes such as
TemporalStore can attach their own service labels without duplicating the
capability-matrix logic.

Production readiness also requires real ByteRaft benchmark evidence. Model
benchmark runners remain available for unit tests, but
`rustraft_production_readiness_report()` blocks production claims unless
benchmark evidence proves a real ByteRaft harness and the RustRaft runtime ran
the same 3-node workloads and passed correctness plus the configured latency and
throughput threshold.

## Why It Lives Separately

Keeping RustRaft in a separate repository gives TemporalStore a stable
consensus-readiness boundary:

- TemporalStore can consume a pinned RustRaft revision.
- Future RustRaft state-machine, transport, snapshot, and membership traits can
  be added without burying them inside the TemporalStore application crate.
- Shared tests can validate the contract independently from production storage
  process wiring.

## Current Scope

This standalone version is the Rust equivalent of the C++ TemporalStore +
ByteRaft split: RustRaft owns the reusable Raft-facing contracts and model
primitives, while TemporalStore owns only FSM/domain adapters, codecs, process
wiring, and storage-engine integration. RustRaft now owns the stable
node/options, storage, transport, status, metric, safety-policy, WAL record,
snapshot fence, membership, and ByteRaft-parity surfaces that TemporalStore can
consume from data-node and metaserver code. The remaining roadmap is tracked in
[`docs/gap_plan.md`](docs/gap_plan.md).

Read safety now includes structured report types for quorum, applied-index
fences, leader lease-read eligibility, and bounded-stale follower reads. These
reports are intended to be filled with observed process-path evidence before a
TemporalStore deployment claims ByteRaft-style read-index or lease-read parity.

Transport contracts include fail-fast request/response validators and a generic
in-memory transport router. The router is meant for library tests and harness
adapters; production TemporalStore still owns real process transports and
durable FSM adapters.

The `rustraft_temporalstore_extraction_plan()` API is the typed migration
ledger. It records which Raft responsibilities are already owned by this
standalone crate, which remain pending migration, and which must stay as
TemporalStore-specific adapters.

## Open Source Surface

RustRaft exposes its standalone boundary through public modules for `node`,
`cluster`, `membership`, `wal`, `snapshot`, `transport`, `status`, `metrics`,
`readiness`, `storage`, `benchmark`, and `fault`. The
`rustraft_open_source_surface()` report names those modules, embedding examples,
ByteRaft parity matrix entries, benchmark harness APIs, and compatibility
reports so consumers can check the published surface without scraping docs.

RustRaft owns generic Raft contracts, parity/readiness reports, benchmark
interfaces, transport/storage/state-machine traits, and status/metrics surfaces.
TemporalStore keeps adapter docs and implementation details for command codecs,
TemporalEngine apply logic, metaserver scheduling, HTTP/process endpoints, and
storage-object wiring.

`rustraft_standalone_readiness_report()` is the fail-closed status check for a
non-TemporalStore embedding. It only reports `ProductionReady` when the public
crate surface covers node lifecycle, replication, election/pre-vote, membership,
WAL recovery, snapshots, read-index/lease-read, and status/metrics/readiness
without relying on TemporalStore adapter code.

`tests/standalone_embedding_contract.rs` repeats that status check as five
executable embedding passes: node lifecycle, replication/read safety,
membership workflow, WAL/snapshot durability, and final readiness/API coverage.
Those tests are the guardrail for continuing to move generic Raft substrate out
of TemporalStore and into this standalone crate.

The intended TemporalStore adapter shape is:

```rust
struct TemporalRaftConsensusBackend {
    node: rustraft::node::RaftNodeRuntime<TemporalStoreStateMachine, TemporalTransport>,
    codec: TemporalCommandCodec,
    engine: TemporalEngine,
}
```

`rustraft_temporalstore_adapter_shape()` exposes this as a typed compatibility
report. RustRaft owns consensus behavior inside the node runtime; TemporalStore
owns command encoding, apply semantics, storage engine integration, and
process/admin surfaces.

The fault-harness API names the ByteRaft-derived process scenarios that
TemporalStore must prove with spawned data-node and metaserver processes:
packet loss, slow WAL fsync, snapshot during membership change, leader transfer
under load, follower rejoin after compacted logs, and rolling restart with
pending joint consensus.

## Test

```bash
cargo test
```

Run the five-pass standalone embedding contract:

```bash
cargo test --test standalone_embedding_contract
```

## Examples

The examples are intentionally storage/runtime agnostic. They show how an
application such as TemporalStore should feed process evidence into the
standalone RustRaft contract.

Build and run the readiness report example:

```bash
cargo run --example readiness_report
```

Run the read-safety policy example:

```bash
cargo run --example read_safety
```

Inspect the open-source embedding surface:

```bash
cargo run --example open_source_surface
```

Run the standalone ByteRaft-vs-RustRaft benchmark script from the RustRaft repo:

```bash
BYTERAFT_ROOT=/path/to/byteraft \
  bash scripts/byteraft_vs_rustraft_benchmark.sh \
  --out target/byteraft-vs-rustraft-benchmark/report.json
```

The script does not enter or depend on the TemporalStore checkout. It fails
closed with `benchmark:real_byteraft_missing` unless `BYTERAFT_ROOT` contains a
`byteraft_parity_benchmark` harness or `BYTERAFT_BENCHMARK_BIN` points to one.
The model runner is intentionally not used for production parity.

These examples are also covered by integration tests so the public snippets stay
in sync with the crate API.
