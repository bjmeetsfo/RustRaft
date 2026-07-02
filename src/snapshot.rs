//! Snapshot model, snapshot lifecycle, install validation, and persistent snapshot store API.

pub use crate::{
    rustraft_snapshot_lifecycle_evidence, rustraft_validate_apply_snapshot_fence,
    rustraft_validate_snapshot_floor_log_matching, rustraft_validate_snapshot_install,
    InstallSnapshotRequest, InstallSnapshotResponse, PersistentRaftSnapshotStore,
    PersistentRaftSnapshotStoreOptions, RaftSnapshot, RaftSnapshotInstallState,
    RaftSnapshotLifecycle, RaftSnapshotLifecycleConfig, RaftSnapshotLifecycleStatus,
    RaftSnapshotSendState, RustRaftApplySnapshotFence, RustRaftGenericSnapshot,
    RustRaftGenericSnapshotChunk, RustRaftInstallSnapshotRequest, RustRaftInstallSnapshotResponse,
    RustRaftSnapshotChunk, RustRaftSnapshotLifecycleEvidence, RustRaftSnapshotMeta, SnapshotChunk,
};
