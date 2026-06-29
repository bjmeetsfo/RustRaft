# RustRaft

RustRaft is the TemporalStore-owned Rust Raft readiness and parity contract
library. It is intentionally small: the crate owns the stable public contract
for RustRaft semantic requirements, readiness evidence, and parity reports,
while TemporalStore owns the storage runtime, data-node integration, and
metaserver integration.

## What This Crate Provides

- `RustRaftSemanticRequirement`
- `RustRaftParityContract`
- `RustRaftParityReport`
- `RustRaftReadinessEvidence`
- `RustRaftReadinessSnapshot`
- `rustraft_parity_contract`
- `rustraft_parity_report`

The crate is independent of OpenRaft types. TemporalStore converts its internal
readiness evidence into `RustRaftReadinessSnapshot` or implements
`RustRaftReadinessEvidence`, then asks this crate to build a conservative parity
report.

## Why It Lives Separately

Keeping RustRaft in a separate repository gives TemporalStore a stable
consensus-readiness boundary:

- TemporalStore can consume a pinned RustRaft revision.
- Future RustRaft state-machine, transport, snapshot, and membership traits can
  be added without burying them inside the TemporalStore application crate.
- Shared tests can validate the contract independently from production storage
  process wiring.

## Current Scope

This first standalone version is a contract library. It does not yet implement a
complete Raft consensus runtime. The remaining roadmap is tracked in
[`docs/gap_plan.md`](docs/gap_plan.md).

## Test

```bash
cargo test
```
