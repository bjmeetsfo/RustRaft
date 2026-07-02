//! WAL records, segmented recovery, checksums, and persistent local WAL API.

pub use crate::{
    rustraft_durability_parity_report, rustraft_recover_latest_wal_record,
    rustraft_validate_hard_state_persistence, rustraft_wal_checksum, rustraft_wal_checksum_valid,
    rustraft_wal_lifecycle_evidence, FileRaftWal, LocalRaftWal, PersistentRaftWal,
    PersistentRaftWalOptions, RaftHardState, RaftWalRecord, RaftWalRecoveryReport, RaftWalSegment,
    RustRaftDurabilityParityReport, RustRaftHardState, RustRaftWalLifecycleEvidence,
    RustRaftWalLifecycleStatus, RustRaftWalRecord,
};
