# RustRaft Gap Plan

RustRaft is the TemporalStore-owned Rust Raft compatibility and readiness layer.
The goal is to make TemporalStore independent of legacy upstream Raft naming while
keeping the operational semantics that production storage needs: leader-only
writes, bounded reads, durable hard state, safe membership, snapshots, failover,
and observable apply lag.

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
- The library now owns stable production-boundary APIs for:
  - `RustRaftStorage`
  - `RustRaftTransport`
  - AppendEntries, Vote, InstallSnapshot, and ReadIndex request/response messages
  - `RustRaftStatusSnapshot`
  - `RustRaftMetricNames`
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
| Native log runtime | The contract is now separate, but runtime code still lives inside `temporalstore-rust`. | Move reusable log entry, hard-state, membership, snapshot-floor, and read-index primitives into this repo. | RustRaft unit tests plus TemporalStore integration tests. |
| Transport abstraction | Stable RustRaft transport API exists; production data-node and metaserver paths still need to implement it directly. | Wire `RustRaftTransport` into TemporalStore data-node and metaserver RPC paths. | Shared Raft transport contract cases. |
| Snapshot lifecycle | Snapshot floor, chunk retry, stale chunk rejection, and tail catch-up are still tested mostly through TemporalStore. | Add library-level snapshot state machine and fault tests. | `raft_rustraft_snapshot_lifecycle_depth`. |
| Membership workflow | Learner catch-up, promote, remove, transfer leader, and joint membership need a reusable library state model. | Add membership planner/state transitions to this repo; TemporalStore metaserver consumes it. | `raft_rustraft_leader_transfer_high_write_fault_harness` and membership cases. |
| Metrics model | RustRaft metric names and status snapshots exist; runtime exporters still need to emit them everywhere. | Wire `RustRaftMetricNames` and `RustRaftStatusSnapshot` into TemporalStore metrics/admin endpoints. | Grafana/Prometheus parity checks. |
| Fault harness API | Fault cases are currently driven by TemporalStore harnesses. | Add a library-level deterministic harness for partitions, packet loss, slow WAL, restart, compaction, and snapshot install. | `raft_rustraft_*_fault_harness` cases. |
| Storage adapter boundary | Stable RustRaft storage trait exists; durable storage implementation remains TemporalStore-specific. | Implement `RustRaftStorage` for TemporalStore log/snapshot storage adapters. | Storage recovery and compaction gates. |

## Implementation Order

1. Keep this repo as the stable public RustRaft contract crate.
2. Move pure contract/state types first; keep TemporalStore process and storage code
   where it is until the library boundary is stable.
3. Add RustRaft transport and storage traits without changing production behavior. Done.
4. Add library-level deterministic state-machine tests for read-index, stale leader,
   learner promotion, snapshot floor, and compacted-entry rejection.
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
