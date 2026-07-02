//! WAL records, segmented recovery, checksums, and persistent local WAL API.

pub use crate::{
    rustraft_recover_latest_wal_record, rustraft_wal_checksum, rustraft_wal_checksum_valid,
    rustraft_wal_lifecycle_evidence, FileRaftWal, LocalRaftWal, PersistentRaftWal,
    PersistentRaftWalOptions, RaftHardState, RaftWalRecord, RaftWalRecoveryReport, RaftWalSegment,
    RustRaftHardState, RustRaftWalLifecycleEvidence, RustRaftWalLifecycleStatus, RustRaftWalRecord,
};
