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
- `RustRaftProductionStatus`
- `RustRaftStorage`
- `RustRaftTransport`
- `RustRaftStatusSnapshot`
- `RustRaftMetricNames`
- `rustraft_read_safety_decision`
- `rustraft_learner_promotion_decision`
- `rustraft_append_safety_decision`
- `RustRaftReadinessEvidence`
- `RustRaftReadinessSnapshot`
- `rustraft_parity_contract`
- `rustraft_parity_report`
- `rustraft_production_readiness_report`
- `rustraft_public_api_contract`
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

The `rustraft_temporalstore_extraction_plan()` API is the typed migration
ledger. It records which Raft responsibilities are already owned by this
standalone crate, which remain pending migration, and which must stay as
TemporalStore-specific adapters.

## Test

```bash
cargo test
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

Both examples are also covered by integration tests so the public snippets stay
in sync with the crate API.
