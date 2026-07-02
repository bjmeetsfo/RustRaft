# RustRaft Gap Plan

RustRaft is the TemporalStore-owned Rust Raft compatibility and readiness layer.
The goal is to make TemporalStore independent of legacy upstream Raft naming while
keeping the operational semantics that production storage needs: leader-only
writes, bounded reads, durable hard state, safe membership, snapshots, failover,
and observable apply lag.

The intended architecture mirrors C++ TemporalStore consuming ByteRaft:
RustRaft owns generic Raft mechanics and public contracts; Rust TemporalStore
keeps only adapters for TemporalStore commands, metaserver mutations, process
startup, object/page storage, and admin endpoints.

## What Is Implemented Now

- `RustRaft` lives in its own repository:
  `https://github.com/bjmeetsfo/RustRaft`.
- `crates/temporalstore-rust` consumes this external crate through a pinned git
  dependency.
- The library owns:
  - `RustRaftSemanticRequirement`
  - `RustRaftParityContract`
  - `RustRaftParityReport`
  - `RustRaftProductionStatus`
  - `RustRaftReadinessEvidence`
  - `RustRaftReadinessSnapshot`
  - `rustraft_parity_contract`
  - `rustraft_parity_report`
- `temporalstore-rust` converts `RaftDistributedReadiness` into a
  `RustRaftReadinessSnapshot`, then asks the `rustraft` library to build the
  report.
- Data-node and metaserver process-rollout evidence can be validated directly
  with `rustraft_data_node_process_rollout_readiness_report()` and
  `rustraft_meta_process_rollout_readiness_report()`.
- ByteRaft-derived runtime capability evidence can be validated with
  `rustraft_byteraft_runtime_capability_report()`, which fails closed on
  missing process-path rollout proof, per-peer pipeline state, reorder queue
  semantics, snapshot sender/downloader lifecycle, WAL segment lifecycle,
  read-index/lease safety, membership role semantics, FSM apply atomicity, and
  admin/metrics observability.
- Real ByteRaft benchmark evidence is now a production readiness input. The
  benchmark gate fails closed when the ByteRaft side or RustRaft side reports a
  model source instead of a real ByteRaft harness plus RustRaft runtime runner.
- Generic Prometheus text output for the same capability families is available
  through `rustraft_byteraft_runtime_capability_prometheus()`, reducing the
  product-local metric logic TemporalStore needs to carry.
- `rustraft_temporalstore_adapter_shape()` records the desired consumer shape:
  `TemporalRaftConsensusBackend` owns a
  `rustraft::node::RaftNodeRuntime<TemporalStoreStateMachine, TemporalTransport>`
  plus TemporalStore-owned command codec and engine fields.
- The library now owns stable production-boundary APIs for:
  - `RustRaftConsensus`
  - `RustRaftStateMachine`
  - `RustRaftNodeOptions`
  - `RustRaftConfig`
  - voter, learner, and witness roles
  - generic membership, joint membership, WAL record, and snapshot fence types
  - `RustRaftStorage`
  - `RustRaftTransport`
  - transport request/response validation reports
  - generic in-memory transport routing for library tests and harness adapters
  - AppendEntries, Vote, InstallSnapshot, and ReadIndex request/response messages
  - `RustRaftStatusSnapshot`
  - `RustRaftMetricNames`
  - ByteRaft parity-surface reporting
  - read-path reports for quorum, applied-index fences, lease-read eligibility,
    bounded-stale follower reads, and stale leader lease rejection
- Shared corpus and Rust tests use `raft_rustraft_*` case names.
- OpenRaft is not part of the RustRaft contract.
- Production readiness is fail-closed. A report is `blocked` when any required
  semantic is missing, and `production_blockers` carries category-qualified
  blocker ids such as `safety:compacted_entry_rejection` or
  `durability:storage_apply_fence`.

## Production Readiness Gates

RustRaft should only be treated as production-ready when all of these categories
are satisfied by TemporalStore data-node and metaserver evidence:

| Category | Required Evidence |
|---|---|
| Safety | leader write authority, snapshot floor/log matching, compacted entry rejection, metaserver snapshot-floor election safety |
| Durability | storage apply fence tied to durable apply index state |
| Transport | AppendEntries, Vote, InstallSnapshot, and ReadIndex contracts |
| Snapshot | trigger policy, apply fence, snapshot plus tail catch-up |
| Membership | learner catch-up before promotion and metaserver-owned membership workflow |
| Observability | operator status for leader, term, commit, apply, peer state, and lag |

The intended CI rule is:

```text
if production_status != production_ready:
  block production Raft claim
  print production_blockers
```

## Remaining Gaps

| Gap | Why It Matters | Target Implementation | Shared Gate |
|---|---|---|---|
| Native log runtime | The contract is now separate, but most live runtime code still lives inside `temporalstore-rust`. | Move reusable log entry, hard-state, membership, snapshot-floor, and process-observed read-path primitives into this repo; keep only FSM adapters in TemporalStore. | RustRaft unit tests plus TemporalStore integration tests. |
| Transport abstraction | Stable RustRaft transport API, validators, and in-memory routing exist; production data-node and metaserver paths still need to implement the same contracts directly. | Wire `RustRaftTransport` into TemporalStore data-node and metaserver RPC paths. | Shared Raft transport contract cases. |
| Snapshot lifecycle | Snapshot floor, chunk retry, stale chunk rejection, and tail catch-up are still tested mostly through TemporalStore. | Add library-level snapshot state machine and fault tests. | `raft_rustraft_snapshot_lifecycle_depth`. |
| Membership workflow | Learner catch-up, promote, remove, transfer leader, and joint membership need a reusable library state model. | Add membership planner/state transitions to this repo; TemporalStore metaserver consumes it. | `raft_rustraft_leader_transfer_high_write_fault_harness` and membership cases. |
| Metrics model | RustRaft metric names and status snapshots exist; runtime exporters still need to emit them everywhere. | Wire `RustRaftMetricNames` and `RustRaftStatusSnapshot` into TemporalStore metrics/admin endpoints. | Grafana/Prometheus parity checks. |
| Fault harness API | Fault cases are currently driven by TemporalStore harnesses. | Add a library-level deterministic harness for partitions, packet loss, slow WAL, restart, compaction, and snapshot install. | `raft_rustraft_*_fault_harness` cases. |
| Storage adapter boundary | Stable RustRaft storage trait exists; durable storage implementation remains TemporalStore-specific. | Implement `RustRaftStorage` for TemporalStore log/snapshot storage adapters. | Storage recovery and compaction gates. |
| Real ByteRaft binary availability | The benchmark runner is wired, but the private ByteRaft checkout/harness may be absent from a machine. | Provide or build `byteraft_parity_benchmark` under `BYTERAFT_ROOT`, or point `BYTERAFT_BENCHMARK_BIN` at it. | `scripts/byteraft_vs_rustraft_benchmark.sh` fails closed with `benchmark:real_byteraft_missing`. |

The public `rustraft_temporalstore_extraction_plan()` function is the source of
truth for this migration ledger. It keeps reusable consensus behavior in
RustRaft while making TemporalStore-specific adapter boundaries explicit.
The public `rustraft_fault_harness_readiness_report()` function is the
fail-closed contract for ByteRaft-derived process-path fault evidence; real
TemporalStore harnesses still need to provide the observed process reports.
The public process-rollout readiness helpers are the matching fail-closed
contract for spawned-process rollout evidence, including independent WAL and
snapshot dirs, process API writes/mutations, read-index responses, restart
recovery, log-store inspection, and operational semantics.
The public ByteRaft runtime capability report is the matching family-level
contract for operational parity claims; it names exact missing evidence fields
instead of accepting coarse API-presence booleans.
Its Prometheus exporter is intentionally generic and uses `rustraft_byteraft_*`
metric names so downstream services can add their own labels or prefixes without
reimplementing the capability matrix.
TemporalStore adapter docs should live with TemporalStore and show how
`TemporalRaftConsensusBackend` wires command encoding, apply semantics, the
storage engine, and process/admin integration around the RustRaft-owned runtime.

## Implementation Order

1. Keep this repo as the stable public RustRaft contract crate.
2. Move pure contract/state types first; keep TemporalStore process and storage code
   where it is until the library boundary is stable. In progress.
3. Add RustRaft transport and storage traits without changing production behavior. Done.
4. Add library-level deterministic state-machine tests for read-index, stale leader,
   learner promotion, snapshot floor, and compacted-entry rejection. Done.
5. Make TemporalStore data-node and metaserver code consume the shared RustRaft
   traits and reports.
6. Run shared corpus gates for data-node Raft, metaserver Raft, multi-node,
   failover, snapshot restore, membership, and read safety.

## Non-Goals For This Step

- This step does not replace the C++ dependency checkout or its upstream build
  flags. Those remain compatibility plumbing until C++ is independently moved to
  a RustRaft-named wrapper.
- This step does not claim production consensus replacement by itself. It creates
  the reusable Rust library boundary and keeps the existing TemporalStore tests as
  the proof path.
