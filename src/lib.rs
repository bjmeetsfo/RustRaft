//! RustRaft readiness and parity contract types.
//!
//! This crate deliberately keeps the public RustRaft contract separate from
//! TemporalStore's data-node and metaserver runtime code. Runtime crates provide
//! readiness evidence; this crate turns that evidence into a stable parity
//! contract and report.

use serde::{Deserialize, Serialize};

pub type RustRaftNodeId = u64;
pub type RustRaftTerm = u64;
pub type RustRaftLogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLogId {
    pub term: RustRaftTerm,
    pub index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftHardState {
    pub current_term: RustRaftTerm,
    pub voted_for: Option<RustRaftNodeId>,
    pub committed: RustRaftLogIndex,
    pub applied: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLogEntry {
    pub log_id: RustRaftLogId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotMeta {
    pub snapshot_id: String,
    pub last_included: RustRaftLogId,
    pub membership_generation: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotChunk {
    pub meta: RustRaftSnapshotMeta,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesRequest {
    pub group_id: u64,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub prev_log_id: Option<RustRaftLogId>,
    pub entries: Vec<RustRaftLogEntry>,
    pub leader_commit: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesResponse {
    pub term: RustRaftTerm,
    pub success: bool,
    pub match_index: RustRaftLogIndex,
    pub conflict_index: Option<RustRaftLogIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteRequest {
    pub group_id: u64,
    pub term: RustRaftTerm,
    pub candidate_id: RustRaftNodeId,
    pub last_log_id: Option<RustRaftLogId>,
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteResponse {
    pub term: RustRaftTerm,
    pub vote_granted: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotRequest {
    pub group_id: u64,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub chunk: RustRaftSnapshotChunk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotResponse {
    pub term: RustRaftTerm,
    pub accepted: bool,
    pub next_offset: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexRequest {
    pub group_id: u64,
    pub requester_id: RustRaftNodeId,
    pub min_commit_index: RustRaftLogIndex,
    pub allow_lease_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexResponse {
    pub term: RustRaftTerm,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub safe: bool,
    pub reason: String,
}

pub trait RustRaftStorage {
    type Error;

    fn append_entries(&mut self, entries: &[RustRaftLogEntry]) -> Result<(), Self::Error>;
    fn read_entries(
        &self,
        start: RustRaftLogIndex,
        end: RustRaftLogIndex,
    ) -> Result<Vec<RustRaftLogEntry>, Self::Error>;
    fn save_hard_state(&mut self, hard_state: &RustRaftHardState) -> Result<(), Self::Error>;
    fn load_hard_state(&self) -> Result<RustRaftHardState, Self::Error>;
    fn save_snapshot(
        &mut self,
        meta: &RustRaftSnapshotMeta,
        bytes: &[u8],
    ) -> Result<(), Self::Error>;
    fn load_snapshot(&self, snapshot_id: &str) -> Result<Vec<u8>, Self::Error>;
    fn tombstone_compacted_entries(
        &mut self,
        compacted_through: RustRaftLogIndex,
    ) -> Result<(), Self::Error>;
}

pub trait RustRaftTransport {
    type Error;

    fn append_entries(
        &self,
        target: RustRaftNodeId,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, Self::Error>;
    fn vote(
        &self,
        target: RustRaftNodeId,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, Self::Error>;
    fn install_snapshot(
        &self,
        target: RustRaftNodeId,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, Self::Error>;
    fn read_index(
        &self,
        target: RustRaftNodeId,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, Self::Error>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRole {
    Leader,
    Follower,
    Candidate,
    Learner,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerStatus {
    pub node_id: RustRaftNodeId,
    pub matched: RustRaftLogIndex,
    pub next_index: RustRaftLogIndex,
    pub learner: bool,
    pub healthy: bool,
    pub lag: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerPipelineStatus {
    pub peer_id: RustRaftNodeId,
    pub match_index: RustRaftLogIndex,
    pub next_index: RustRaftLogIndex,
    pub append_requests: u64,
    pub append_accepted: u64,
    pub append_rejected: u64,
    pub inflight_entries: u64,
    pub inflight_bytes: u64,
    pub append_queue_depth: u64,
    pub append_queue_limit: u64,
    pub append_queue_max_depth: u64,
    pub inflight_bytes_limit: u64,
    pub apply_inflight_tasks: u64,
    pub apply_inflight_limit: u64,
    pub apply_queue_depth: u64,
    pub apply_queue_max_depth: u64,
    pub apply_batch_bytes_limit: u64,
    pub apply_backpressure_rejections: u64,
    pub memory_backpressure_rejections: u64,
    pub oversized_log_rejections: u64,
    pub reorder_queue_depth: u64,
    pub out_of_order_append_rejections: u64,
    pub reorder_entries_rejected: u64,
    pub reorder_entry_timeouts: u64,
    pub reorder_dropped_packages: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: RustRaftLogIndex,
    pub snapshot_send_attempts: u64,
    pub snapshot_install_total_chunks: u64,
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_backpressure_rejections: u64,
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_install_rolled_back: u64,
    pub snapshot_during_membership_change: bool,
    pub snapshot_rejoin_after_compacted_log: bool,
    pub transfer_leader_target: bool,
    pub transfer_leader_timeouts: u64,
    pub pre_vote_rejections: u64,
    pub election_rejections: u64,
    pub offline_timeout_reached: bool,
    pub offline_timeout_rejections: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPipelineLimits {
    pub max_inflights_replicate: u64,
    pub max_memory_replicate_log_bytes: u64,
    pub max_inflights_apply_task: u64,
    pub max_apply_batch_bytes: u64,
    pub enable_reorder_queue: bool,
    pub reorder_window_size: u64,
    pub reorder_timeout_us: u64,
}

impl Default for RustRaftPipelineLimits {
    fn default() -> Self {
        Self {
            max_inflights_replicate: 128,
            max_memory_replicate_log_bytes: 32 * 1024,
            max_inflights_apply_task: 5,
            max_apply_batch_bytes: 64 * 1024,
            enable_reorder_queue: true,
            reorder_window_size: 128,
            reorder_timeout_us: 3_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPipelineEvidence {
    pub per_peer_pipeline_state_present: bool,
    pub append_backpressure_enforced: bool,
    pub apply_backpressure_enforced: bool,
    pub memory_replicate_bytes_enforced: bool,
    pub oversized_log_rejection_present: bool,
    pub reorder_queue_enabled: bool,
    pub out_of_order_append_handling_present: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

pub fn rustraft_pipeline_evidence(
    peers: &[RustRaftPeerPipelineStatus],
    limits: RustRaftPipelineLimits,
) -> RustRaftPipelineEvidence {
    let per_peer_pipeline_state_present = !peers.is_empty()
        && peers.iter().all(|peer| peer.next_index > 0)
        && peers.iter().any(|peer| {
            peer.append_queue_depth > 0 || peer.inflight_entries > 0 || peer.append_requests > 0
        });
    let append_backpressure_enforced = limits.max_inflights_replicate > 0
        && limits.max_memory_replicate_log_bytes > 0
        && peers.iter().any(|peer| {
            peer.append_rejected > 0
                && (peer.inflight_entries > 0
                    || peer.inflight_bytes > 0
                    || peer.append_queue_max_depth > 0)
                && (peer.append_queue_depth >= peer.append_queue_limit
                    || peer.inflight_bytes >= peer.inflight_bytes_limit
                    || peer.append_queue_max_depth >= peer.append_queue_limit)
        });
    let apply_backpressure_enforced = limits.max_apply_batch_bytes > 0
        && limits.max_inflights_apply_task > 0
        && peers.iter().any(|peer| {
            peer.apply_backpressure_rejections > 0
                && peer.apply_inflight_limit > 0
                && peer.apply_batch_bytes_limit > 0
                && (peer.apply_queue_depth >= peer.apply_inflight_limit
                    || peer.apply_queue_max_depth >= peer.apply_inflight_limit)
        });
    let memory_replicate_bytes_enforced = limits.max_memory_replicate_log_bytes > 0
        && peers
            .iter()
            .any(|peer| peer.memory_backpressure_rejections > 0);
    let oversized_log_rejection_present =
        peers.iter().any(|peer| peer.oversized_log_rejections > 0);
    let reorder_queue_enabled = limits.enable_reorder_queue
        && limits.reorder_window_size > 0
        && limits.reorder_timeout_us > 0;
    let out_of_order_append_handling_present = peers.iter().any(|peer| {
        peer.out_of_order_append_rejections > 0
            || peer.reorder_entries_rejected > 0
            || peer.reorder_entry_timeouts > 0
            || peer.reorder_dropped_packages > 0
    });
    let mut blockers = Vec::new();
    if !per_peer_pipeline_state_present {
        blockers.push("per_peer_pipeline_state_missing".to_string());
    }
    if !append_backpressure_enforced {
        blockers.push("append_backpressure_not_enforced".to_string());
    }
    if !apply_backpressure_enforced {
        blockers.push("apply_backpressure_not_enforced".to_string());
    }
    if !memory_replicate_bytes_enforced {
        blockers.push("memory_replicate_bytes_not_enforced".to_string());
    }
    if !oversized_log_rejection_present {
        blockers.push("oversized_log_rejection_missing".to_string());
    }
    if !reorder_queue_enabled {
        blockers.push("reorder_queue_not_enabled".to_string());
    }
    if !out_of_order_append_handling_present {
        blockers.push("out_of_order_append_handling_missing".to_string());
    }
    RustRaftPipelineEvidence {
        per_peer_pipeline_state_present,
        append_backpressure_enforced,
        apply_backpressure_enforced,
        memory_replicate_bytes_enforced,
        oversized_log_rejection_present,
        reorder_queue_enabled,
        out_of_order_append_handling_present,
        ready: blockers.is_empty(),
        blockers,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotLifecycleEvidence {
    pub sender_lifecycle_present: bool,
    pub downloader_lifecycle_present: bool,
    pub retry_backpressure_present: bool,
    pub rate_limit_present: bool,
    pub install_progress_present: bool,
    pub install_rollback_present: bool,
    pub membership_change_present: bool,
    pub rejoin_after_compacted_log_present: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

pub fn rustraft_snapshot_lifecycle_evidence(
    peers: &[RustRaftPeerPipelineStatus],
    send_snapshot_timeout_ms: u64,
    max_inflights_replicate: u64,
) -> RustRaftSnapshotLifecycleEvidence {
    let sender_lifecycle_present = peers
        .iter()
        .any(|peer| peer.snapshot_sending || peer.snapshot_installed_index > 0);
    let downloader_lifecycle_present = peers
        .iter()
        .any(|peer| peer.snapshot_installing || peer.snapshot_installed_index > 0);
    let retry_backpressure_present = send_snapshot_timeout_ms > 0
        && max_inflights_replicate > 0
        && peers.iter().any(|peer| {
            peer.snapshot_send_attempts > 0
                && peer.snapshot_install_total_chunks > 0
                && peer.snapshot_backpressure_rejections > 0
        });
    let rate_limit_present = peers
        .iter()
        .any(|peer| peer.snapshot_rate_limit_rejections > 0)
        || max_inflights_replicate > 0;
    let install_progress_present = peers.iter().any(|peer| {
        peer.snapshot_install_total_chunks > 0 && peer.snapshot_install_progress_per_mille > 0
    });
    let install_rollback_present = peers
        .iter()
        .any(|peer| peer.snapshot_install_rolled_back > 0);
    let membership_change_present = peers
        .iter()
        .any(|peer| peer.snapshot_during_membership_change);
    let rejoin_after_compacted_log_present = peers
        .iter()
        .any(|peer| peer.snapshot_rejoin_after_compacted_log);
    let mut blockers = Vec::new();
    if !sender_lifecycle_present {
        blockers.push("snapshot_sender_lifecycle_missing".to_string());
    }
    if !downloader_lifecycle_present {
        blockers.push("snapshot_downloader_lifecycle_missing".to_string());
    }
    if !retry_backpressure_present {
        blockers.push("snapshot_retry_backpressure_missing".to_string());
    }
    if !rate_limit_present {
        blockers.push("snapshot_rate_limit_missing".to_string());
    }
    if !install_progress_present {
        blockers.push("snapshot_install_progress_missing".to_string());
    }
    if !install_rollback_present {
        blockers.push("snapshot_install_rollback_missing".to_string());
    }
    if !membership_change_present {
        blockers.push("snapshot_membership_change_missing".to_string());
    }
    if !rejoin_after_compacted_log_present {
        blockers.push("snapshot_rejoin_after_compacted_log_missing".to_string());
    }
    RustRaftSnapshotLifecycleEvidence {
        sender_lifecycle_present,
        downloader_lifecycle_present,
        retry_backpressure_present,
        rate_limit_present,
        install_progress_present,
        install_rollback_present,
        membership_change_present,
        rejoin_after_compacted_log_present,
        ready: blockers.is_empty(),
        blockers,
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleStatus {
    pub segment_count: u64,
    pub active_segment_id: u64,
    pub first_retained_segment_id: u64,
    pub last_retained_segment_id: u64,
    pub total_bytes: u64,
    pub active_segment_bytes: u64,
    pub total_records: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub first_log_index: RustRaftLogIndex,
    pub last_log_index: RustRaftLogIndex,
    pub released_segment_count: u64,
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleEvidence {
    pub segment_lifecycle_present: bool,
    pub release_rules_observed: bool,
    pub first_last_index_status_present: bool,
    pub slow_fsync_backpressure_observed: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

pub fn rustraft_wal_lifecycle_evidence(
    status: &RustRaftWalLifecycleStatus,
) -> RustRaftWalLifecycleEvidence {
    let segment_lifecycle_present = status.segment_count > 0
        && status.active_segment_id >= status.first_retained_segment_id
        && status.last_retained_segment_id >= status.first_retained_segment_id
        && status.total_bytes > 0
        && status.active_segment_bytes > 0
        && status.total_records > 0
        && status.last_sequence >= status.first_sequence
        && status.last_log_index >= status.first_log_index;
    let release_rules_observed = status.released_segment_count > 0
        || status.last_retained_segment_id >= status.first_retained_segment_id;
    let first_last_index_status_present = status.last_log_index >= status.first_log_index;
    let mut blockers = Vec::new();
    if !segment_lifecycle_present {
        blockers.push("wal_segment_lifecycle_missing".to_string());
    }
    if !release_rules_observed {
        blockers.push("wal_release_rules_missing".to_string());
    }
    if !first_last_index_status_present {
        blockers.push("wal_first_last_index_status_missing".to_string());
    }
    RustRaftWalLifecycleEvidence {
        segment_lifecycle_present,
        release_rules_observed,
        first_last_index_status_present,
        slow_fsync_backpressure_observed: status.slow_fsync_backpressure_observed,
        ready: blockers.is_empty(),
        blockers,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStatusSnapshot {
    pub group_id: u64,
    pub node_id: RustRaftNodeId,
    pub role: RustRaftRole,
    pub term: RustRaftTerm,
    pub leader_id: Option<RustRaftNodeId>,
    pub commit_index: RustRaftLogIndex,
    pub applied_index: RustRaftLogIndex,
    pub last_log_index: RustRaftLogIndex,
    pub last_snapshot_index: RustRaftLogIndex,
    pub peers: Vec<RustRaftPeerStatus>,
}

impl RustRaftStatusSnapshot {
    pub fn apply_lag(&self) -> u64 {
        self.commit_index.saturating_sub(self.applied_index)
    }

    pub fn peer(&self, node_id: RustRaftNodeId) -> Option<&RustRaftPeerStatus> {
        self.peers.iter().find(|peer| peer.node_id == node_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyDecision {
    pub safe: bool,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub reason: String,
}

pub fn rustraft_read_safety_decision(
    status: &RustRaftStatusSnapshot,
    request: &RustRaftReadIndexRequest,
) -> RustRaftReadSafetyDecision {
    if !matches!(status.role, RustRaftRole::Leader | RustRaftRole::Follower) {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "role_not_readable".to_string(),
        };
    }
    if status.commit_index < request.min_commit_index {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "commit_index_too_low".to_string(),
        };
    }
    if status.applied_index < request.min_commit_index {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.applied_index,
            lease_read: false,
            reason: "apply_lag_too_high".to_string(),
        };
    }
    if status.last_snapshot_index > request.min_commit_index {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.last_snapshot_index,
            lease_read: false,
            reason: "read_before_snapshot_floor".to_string(),
        };
    }
    RustRaftReadSafetyDecision {
        safe: true,
        read_index: request.min_commit_index,
        lease_read: request.allow_lease_read && matches!(status.role, RustRaftRole::Leader),
        reason: "read_safe".to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLearnerPromotionDecision {
    pub promotable: bool,
    pub reason: String,
}

pub fn rustraft_learner_promotion_decision(
    status: &RustRaftStatusSnapshot,
    learner_id: RustRaftNodeId,
    max_lag: u64,
) -> RustRaftLearnerPromotionDecision {
    let Some(peer) = status.peer(learner_id) else {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            reason: "peer_not_found".to_string(),
        };
    };
    if !peer.learner {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            reason: "peer_not_learner".to_string(),
        };
    }
    if !peer.healthy {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            reason: "peer_unhealthy".to_string(),
        };
    }
    if peer.lag > max_lag || peer.matched.saturating_add(max_lag) < status.commit_index {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            reason: "learner_lag_too_high".to_string(),
        };
    }
    RustRaftLearnerPromotionDecision {
        promotable: true,
        reason: "learner_caught_up".to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendSafetyDecision {
    pub accepted: bool,
    pub reason: String,
}

pub fn rustraft_append_safety_decision(
    snapshot_floor: RustRaftLogIndex,
    first_retained_log_index: RustRaftLogIndex,
    request: &RustRaftAppendEntriesRequest,
) -> RustRaftAppendSafetyDecision {
    let prev_index = request
        .prev_log_id
        .as_ref()
        .map(|log| log.index)
        .unwrap_or(0);
    if prev_index != 0 && prev_index < snapshot_floor {
        return RustRaftAppendSafetyDecision {
            accepted: false,
            reason: "prev_log_before_snapshot_floor".to_string(),
        };
    }
    if let Some(entry) = request
        .entries
        .iter()
        .find(|entry| entry.log_id.index < first_retained_log_index)
    {
        return RustRaftAppendSafetyDecision {
            accepted: false,
            reason: format!("entry_compacted:{}", entry.log_id.index),
        };
    }
    RustRaftAppendSafetyDecision {
        accepted: true,
        reason: "append_safe".to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetricNames {
    pub leader_changes_total: String,
    pub append_entries_qps: String,
    pub append_entries_latency_ms: String,
    pub read_index_latency_ms: String,
    pub apply_lag: String,
    pub snapshot_install_total: String,
    pub snapshot_install_latency_ms: String,
    pub membership_change_total: String,
    pub transport_errors_total: String,
}

pub fn rustraft_metric_names() -> RustRaftMetricNames {
    RustRaftMetricNames {
        leader_changes_total: "rustraft_leader_changes_total".to_string(),
        append_entries_qps: "rustraft_append_entries_qps".to_string(),
        append_entries_latency_ms: "rustraft_append_entries_latency_ms".to_string(),
        read_index_latency_ms: "rustraft_read_index_latency_ms".to_string(),
        apply_lag: "rustraft_apply_lag".to_string(),
        snapshot_install_total: "rustraft_snapshot_install_total".to_string(),
        snapshot_install_latency_ms: "rustraft_snapshot_install_latency_ms".to_string(),
        membership_change_total: "rustraft_membership_change_total".to_string(),
        transport_errors_total: "rustraft_transport_errors_total".to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPublicApiContract {
    pub storage_trait: String,
    pub transport_trait: String,
    pub status_snapshot: String,
    pub metric_namespace: String,
    pub rpc_messages: Vec<String>,
}

pub fn rustraft_public_api_contract() -> RustRaftPublicApiContract {
    RustRaftPublicApiContract {
        storage_trait: "RustRaftStorage".to_string(),
        transport_trait: "RustRaftTransport".to_string(),
        status_snapshot: "RustRaftStatusSnapshot".to_string(),
        metric_namespace: "rustraft".to_string(),
        rpc_messages: vec![
            "RustRaftAppendEntriesRequest".to_string(),
            "RustRaftVoteRequest".to_string(),
            "RustRaftInstallSnapshotRequest".to_string(),
            "RustRaftReadIndexRequest".to_string(),
        ],
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSemanticRequirement {
    pub id: String,
    pub description: String,
    pub readiness_field: String,
    pub category: RustRaftRequirementCategory,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRequirementCategory {
    Safety,
    Durability,
    Transport,
    Snapshot,
    Membership,
    Observability,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityContract {
    pub consensus_backend_boundary: String,
    pub data_node_backend_trait: String,
    pub metaserver_backend_trait: String,
    pub openraft_dependency_removed: bool,
    pub temporal_raft_runtime_available: bool,
    pub requirements: Vec<RustRaftSemanticRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityReport {
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub contract: RustRaftParityContract,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftProductionStatus {
    Blocked,
    FeatureCorrect,
    ProductionReady,
}

pub trait RustRaftReadinessEvidence {
    fn readiness_value(&self, field: &str) -> bool;
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadinessSnapshot {
    pub rustraft_leader_write_authority_present: bool,
    pub rustraft_operator_observability_present: bool,
    pub rustraft_rpc_transport_contract_present: bool,
    pub rustraft_log_retention_snapshot_trigger_present: bool,
    pub rustraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub rustraft_snapshot_floor_log_matching_present: bool,
    pub rustraft_snapshot_tail_catchup_present: bool,
    pub rustraft_compacted_entry_rejection_present: bool,
    pub rustraft_metaserver_snapshot_floor_election_present: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
}

impl RustRaftReadinessEvidence for RustRaftReadinessSnapshot {
    fn readiness_value(&self, field: &str) -> bool {
        match field {
            "rustraft_leader_write_authority_present" => {
                self.rustraft_leader_write_authority_present
            }
            "rustraft_operator_observability_present" => {
                self.rustraft_operator_observability_present
            }
            "rustraft_rpc_transport_contract_present" => {
                self.rustraft_rpc_transport_contract_present
            }
            "rustraft_log_retention_snapshot_trigger_present" => {
                self.rustraft_log_retention_snapshot_trigger_present
            }
            "rustraft_apply_snapshot_fence_present" => self.rustraft_apply_snapshot_fence_present,
            "raft_storage_apply_fence_present" => self.raft_storage_apply_fence_present,
            "rustraft_snapshot_floor_log_matching_present" => {
                self.rustraft_snapshot_floor_log_matching_present
            }
            "rustraft_snapshot_tail_catchup_present" => self.rustraft_snapshot_tail_catchup_present,
            "rustraft_compacted_entry_rejection_present" => {
                self.rustraft_compacted_entry_rejection_present
            }
            "rustraft_metaserver_snapshot_floor_election_present" => {
                self.rustraft_metaserver_snapshot_floor_election_present
            }
            "learner_catchup_promotion_present" => self.learner_catchup_promotion_present,
            "metaserver_membership_workflow_present" => self.metaserver_membership_workflow_present,
            _ => false,
        }
    }
}

pub fn rustraft_parity_contract() -> RustRaftParityContract {
    RustRaftParityContract {
        consensus_backend_boundary:
            "temporalstore_rust::raft::DataRaftConsensusBackend".to_string(),
        data_node_backend_trait: "DataRaftConsensusBackend".to_string(),
        metaserver_backend_trait: "DataRaftConsensusBackend".to_string(),
        openraft_dependency_removed: true,
        temporal_raft_runtime_available: true,
        requirements: vec![
            requirement(
                "leader_write_authority",
                "Leader-only writes and bounded stale-read authority match RustRaft semantics.",
                "rustraft_leader_write_authority_present",
                RustRaftRequirementCategory::Safety,
            ),
            requirement(
                "operator_observability",
                "Operator-facing status exposes leader, term, commit, apply, and peer state.",
                "rustraft_operator_observability_present",
                RustRaftRequirementCategory::Observability,
            ),
            requirement(
                "rpc_transport_contract",
                "AppendEntries, Vote, InstallSnapshot, and ReadIndex transport contracts exist.",
                "rustraft_rpc_transport_contract_present",
                RustRaftRequirementCategory::Transport,
            ),
            requirement(
                "snapshot_trigger",
                "Log retention can trigger durable snapshots before unbounded growth.",
                "rustraft_log_retention_snapshot_trigger_present",
                RustRaftRequirementCategory::Snapshot,
            ),
            requirement(
                "apply_snapshot_fence",
                "Snapshot install has an apply fence so stale logs cannot overwrite restored state.",
                "rustraft_apply_snapshot_fence_present",
                RustRaftRequirementCategory::Snapshot,
            ),
            requirement(
                "storage_apply_fence",
                "Storage mutation apply is fenced with durable apply index state.",
                "raft_storage_apply_fence_present",
                RustRaftRequirementCategory::Durability,
            ),
            requirement(
                "snapshot_floor_log_matching",
                "Snapshot floor and log matching reject unsafe stale or compacted entries.",
                "rustraft_snapshot_floor_log_matching_present",
                RustRaftRequirementCategory::Safety,
            ),
            requirement(
                "snapshot_tail_catchup",
                "Followers can catch up from snapshot plus tail logs.",
                "rustraft_snapshot_tail_catchup_present",
                RustRaftRequirementCategory::Snapshot,
            ),
            requirement(
                "compacted_entry_rejection",
                "Compacted entries are rejected rather than silently replayed.",
                "rustraft_compacted_entry_rejection_present",
                RustRaftRequirementCategory::Safety,
            ),
            requirement(
                "metaserver_snapshot_floor_election",
                "Metaserver election/readiness respects snapshot floor safety.",
                "rustraft_metaserver_snapshot_floor_election_present",
                RustRaftRequirementCategory::Safety,
            ),
            requirement(
                "learner_catchup_promotion",
                "Learners are promoted only after catch-up and membership workflow checks.",
                "learner_catchup_promotion_present",
                RustRaftRequirementCategory::Membership,
            ),
            requirement(
                "metaserver_membership_workflow",
                "Metaserver owns membership workflow and topology placement transitions.",
                "metaserver_membership_workflow_present",
                RustRaftRequirementCategory::Membership,
            ),
        ],
    }
}

pub fn rustraft_parity_report<E: RustRaftReadinessEvidence>(readiness: &E) -> RustRaftParityReport {
    let contract = rustraft_parity_contract();
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    let mut production_blockers = Vec::new();
    for requirement in &contract.requirements {
        if readiness.readiness_value(&requirement.readiness_field) {
            satisfied.push(requirement.id.clone());
        } else {
            missing.push(requirement.id.clone());
            if requirement.required_for_production {
                production_blockers.push(format!(
                    "{}:{}",
                    requirement.category.as_str(),
                    requirement.id
                ));
            }
        }
    }
    let ready = missing.is_empty() && contract.openraft_dependency_removed;
    let production_status =
        if !contract.openraft_dependency_removed || !production_blockers.is_empty() {
            RustRaftProductionStatus::Blocked
        } else if ready && contract.temporal_raft_runtime_available {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::FeatureCorrect
        };
    RustRaftParityReport {
        ready,
        production_status,
        contract,
        satisfied,
        missing,
        production_blockers,
    }
}

pub fn rustraft_parity_report_from_snapshot(
    readiness: &RustRaftReadinessSnapshot,
) -> RustRaftParityReport {
    rustraft_parity_report(readiness)
}

impl RustRaftRequirementCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            RustRaftRequirementCategory::Safety => "safety",
            RustRaftRequirementCategory::Durability => "durability",
            RustRaftRequirementCategory::Transport => "transport",
            RustRaftRequirementCategory::Snapshot => "snapshot",
            RustRaftRequirementCategory::Membership => "membership",
            RustRaftRequirementCategory::Observability => "observability",
        }
    }
}

fn requirement(
    id: &str,
    description: &str,
    readiness_field: &str,
    category: RustRaftRequirementCategory,
) -> RustRaftSemanticRequirement {
    RustRaftSemanticRequirement {
        id: id.to_string(),
        description: description.to_string(),
        readiness_field: readiness_field.to_string(),
        category,
        required_for_production: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct MemoryRustRaftStorage {
        entries: BTreeMap<RustRaftLogIndex, RustRaftLogEntry>,
        hard_state: RustRaftHardState,
        snapshots: BTreeMap<String, Vec<u8>>,
        compacted_through: RustRaftLogIndex,
    }

    impl RustRaftStorage for MemoryRustRaftStorage {
        type Error = String;

        fn append_entries(&mut self, entries: &[RustRaftLogEntry]) -> Result<(), Self::Error> {
            for entry in entries {
                if entry.log_id.index <= self.compacted_through {
                    return Err("entry_compacted".to_string());
                }
                self.entries.insert(entry.log_id.index, entry.clone());
            }
            Ok(())
        }

        fn read_entries(
            &self,
            start: RustRaftLogIndex,
            end: RustRaftLogIndex,
        ) -> Result<Vec<RustRaftLogEntry>, Self::Error> {
            Ok(self
                .entries
                .range(start..end)
                .map(|(_, entry)| entry.clone())
                .collect())
        }

        fn save_hard_state(&mut self, hard_state: &RustRaftHardState) -> Result<(), Self::Error> {
            self.hard_state = hard_state.clone();
            Ok(())
        }

        fn load_hard_state(&self) -> Result<RustRaftHardState, Self::Error> {
            Ok(self.hard_state.clone())
        }

        fn save_snapshot(
            &mut self,
            meta: &RustRaftSnapshotMeta,
            bytes: &[u8],
        ) -> Result<(), Self::Error> {
            self.snapshots
                .insert(meta.snapshot_id.clone(), bytes.to_vec());
            Ok(())
        }

        fn load_snapshot(&self, snapshot_id: &str) -> Result<Vec<u8>, Self::Error> {
            self.snapshots
                .get(snapshot_id)
                .cloned()
                .ok_or_else(|| "snapshot_not_found".to_string())
        }

        fn tombstone_compacted_entries(
            &mut self,
            compacted_through: RustRaftLogIndex,
        ) -> Result<(), Self::Error> {
            self.compacted_through = compacted_through;
            self.entries
                .retain(|index, _| *index > self.compacted_through);
            Ok(())
        }
    }

    struct LoopbackRustRaftTransport;

    impl RustRaftTransport for LoopbackRustRaftTransport {
        type Error = String;

        fn append_entries(
            &self,
            _target: RustRaftNodeId,
            request: RustRaftAppendEntriesRequest,
        ) -> Result<RustRaftAppendEntriesResponse, Self::Error> {
            Ok(RustRaftAppendEntriesResponse {
                term: request.term,
                success: true,
                match_index: request
                    .entries
                    .last()
                    .map(|entry| entry.log_id.index)
                    .unwrap_or_else(|| request.prev_log_id.map(|log| log.index).unwrap_or(0)),
                conflict_index: None,
            })
        }

        fn vote(
            &self,
            _target: RustRaftNodeId,
            request: RustRaftVoteRequest,
        ) -> Result<RustRaftVoteResponse, Self::Error> {
            Ok(RustRaftVoteResponse {
                term: request.term,
                vote_granted: request.last_log_id.is_some(),
                reason: if request.pre_vote {
                    "pre_vote_checked".to_string()
                } else {
                    "vote_checked".to_string()
                },
            })
        }

        fn install_snapshot(
            &self,
            _target: RustRaftNodeId,
            request: RustRaftInstallSnapshotRequest,
        ) -> Result<RustRaftInstallSnapshotResponse, Self::Error> {
            Ok(RustRaftInstallSnapshotResponse {
                term: request.term,
                accepted: request.chunk.done,
                next_offset: request.chunk.offset + request.chunk.bytes.len() as u64,
                reason: "snapshot_chunk_checked".to_string(),
            })
        }

        fn read_index(
            &self,
            _target: RustRaftNodeId,
            request: RustRaftReadIndexRequest,
        ) -> Result<RustRaftReadIndexResponse, Self::Error> {
            Ok(RustRaftReadIndexResponse {
                term: 7,
                read_index: request.min_commit_index,
                lease_read: request.allow_lease_read,
                safe: true,
                reason: "read_index_checked".to_string(),
            })
        }
    }

    #[test]
    fn contract_contains_production_semantics_without_openraft() {
        let contract = rustraft_parity_contract();
        assert!(contract.openraft_dependency_removed);
        assert!(contract
            .requirements
            .iter()
            .any(|requirement| requirement.id == "leader_write_authority"));
        assert!(contract
            .requirements
            .iter()
            .all(|requirement| requirement.required_for_production));
    }

    #[test]
    fn report_lists_missing_fields() {
        let mut readiness = RustRaftReadinessSnapshot::default();
        readiness.rustraft_leader_write_authority_present = true;

        let report = rustraft_parity_report(&readiness);
        assert!(!report.ready);
        assert!(report
            .satisfied
            .contains(&"leader_write_authority".to_string()));
        assert!(report
            .missing
            .contains(&"operator_observability".to_string()));
        assert_eq!(report.production_status, RustRaftProductionStatus::Blocked);
        assert!(report
            .production_blockers
            .iter()
            .any(|blocker| { blocker == "observability:operator_observability" }));
    }

    #[test]
    fn report_marks_complete_readiness_as_production_ready() {
        let readiness = RustRaftReadinessSnapshot {
            rustraft_leader_write_authority_present: true,
            rustraft_operator_observability_present: true,
            rustraft_rpc_transport_contract_present: true,
            rustraft_log_retention_snapshot_trigger_present: true,
            rustraft_apply_snapshot_fence_present: true,
            raft_storage_apply_fence_present: true,
            rustraft_snapshot_floor_log_matching_present: true,
            rustraft_snapshot_tail_catchup_present: true,
            rustraft_compacted_entry_rejection_present: true,
            rustraft_metaserver_snapshot_floor_election_present: true,
            learner_catchup_promotion_present: true,
            metaserver_membership_workflow_present: true,
        };

        let report = rustraft_parity_report(&readiness);
        assert!(report.ready);
        assert_eq!(
            report.production_status,
            RustRaftProductionStatus::ProductionReady
        );
        assert!(report.production_blockers.is_empty());
    }

    #[test]
    fn public_api_contract_exposes_storage_transport_status_and_metrics() {
        let api = rustraft_public_api_contract();
        assert_eq!(api.storage_trait, "RustRaftStorage");
        assert_eq!(api.transport_trait, "RustRaftTransport");
        assert_eq!(api.status_snapshot, "RustRaftStatusSnapshot");
        assert!(api
            .rpc_messages
            .contains(&"RustRaftAppendEntriesRequest".to_string()));

        let metrics = rustraft_metric_names();
        assert_eq!(metrics.apply_lag, "rustraft_apply_lag");
        assert!(metrics
            .transport_errors_total
            .starts_with(&api.metric_namespace));
    }

    #[test]
    fn storage_trait_covers_hard_state_log_snapshot_and_compaction() {
        let mut storage = MemoryRustRaftStorage::default();
        storage
            .save_hard_state(&RustRaftHardState {
                current_term: 3,
                voted_for: Some(2),
                committed: 4,
                applied: 3,
            })
            .unwrap();
        assert_eq!(storage.load_hard_state().unwrap().current_term, 3);

        storage
            .append_entries(&[
                RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 3, index: 4 },
                    payload: b"set a".to_vec(),
                },
                RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 3, index: 5 },
                    payload: b"set b".to_vec(),
                },
            ])
            .unwrap();
        assert_eq!(storage.read_entries(4, 6).unwrap().len(), 2);

        let meta = RustRaftSnapshotMeta {
            snapshot_id: "snap-5".to_string(),
            last_included: RustRaftLogId { term: 3, index: 5 },
            membership_generation: 9,
            checksum: "sha256:demo".to_string(),
        };
        storage.save_snapshot(&meta, b"snapshot").unwrap();
        assert_eq!(storage.load_snapshot("snap-5").unwrap(), b"snapshot");

        storage.tombstone_compacted_entries(4).unwrap();
        assert_eq!(storage.read_entries(1, 6).unwrap().len(), 1);
        assert!(storage
            .append_entries(&[RustRaftLogEntry {
                log_id: RustRaftLogId { term: 3, index: 4 },
                payload: b"stale".to_vec(),
            }])
            .is_err());
    }

    #[test]
    fn transport_trait_covers_append_vote_snapshot_and_read_index() {
        let transport = LoopbackRustRaftTransport;
        let append = transport
            .append_entries(
                2,
                RustRaftAppendEntriesRequest {
                    group_id: 1,
                    term: 8,
                    leader_id: 1,
                    prev_log_id: Some(RustRaftLogId { term: 7, index: 11 }),
                    entries: vec![RustRaftLogEntry {
                        log_id: RustRaftLogId { term: 8, index: 12 },
                        payload: b"write".to_vec(),
                    }],
                    leader_commit: 12,
                },
            )
            .unwrap();
        assert!(append.success);
        assert_eq!(append.match_index, 12);

        let vote = transport
            .vote(
                2,
                RustRaftVoteRequest {
                    group_id: 1,
                    term: 9,
                    candidate_id: 3,
                    last_log_id: Some(RustRaftLogId { term: 8, index: 12 }),
                    pre_vote: true,
                },
            )
            .unwrap();
        assert!(vote.vote_granted);

        let snapshot = transport
            .install_snapshot(
                2,
                RustRaftInstallSnapshotRequest {
                    group_id: 1,
                    term: 9,
                    leader_id: 1,
                    chunk: RustRaftSnapshotChunk {
                        meta: RustRaftSnapshotMeta {
                            snapshot_id: "snap-12".to_string(),
                            last_included: RustRaftLogId { term: 8, index: 12 },
                            membership_generation: 1,
                            checksum: "sha256:demo".to_string(),
                        },
                        offset: 0,
                        bytes: b"chunk".to_vec(),
                        done: true,
                    },
                },
            )
            .unwrap();
        assert!(snapshot.accepted);

        let read = transport
            .read_index(
                2,
                RustRaftReadIndexRequest {
                    group_id: 1,
                    requester_id: 2,
                    min_commit_index: 12,
                    allow_lease_read: false,
                },
            )
            .unwrap();
        assert!(read.safe);
        assert_eq!(read.read_index, 12);
    }

    #[test]
    fn read_safety_policy_rejects_unapplied_or_compacted_reads() {
        let status = RustRaftStatusSnapshot {
            group_id: 1,
            node_id: 1,
            role: RustRaftRole::Leader,
            term: 4,
            leader_id: Some(1),
            commit_index: 20,
            applied_index: 18,
            last_log_index: 20,
            last_snapshot_index: 9,
            peers: Vec::new(),
        };
        assert_eq!(status.apply_lag(), 2);

        let lagging = rustraft_read_safety_decision(
            &status,
            &RustRaftReadIndexRequest {
                group_id: 1,
                requester_id: 1,
                min_commit_index: 19,
                allow_lease_read: true,
            },
        );
        assert!(!lagging.safe);
        assert_eq!(lagging.reason, "apply_lag_too_high");

        let compacted = rustraft_read_safety_decision(
            &status,
            &RustRaftReadIndexRequest {
                group_id: 1,
                requester_id: 1,
                min_commit_index: 8,
                allow_lease_read: true,
            },
        );
        assert!(!compacted.safe);
        assert_eq!(compacted.reason, "read_before_snapshot_floor");

        let safe = rustraft_read_safety_decision(
            &status,
            &RustRaftReadIndexRequest {
                group_id: 1,
                requester_id: 1,
                min_commit_index: 18,
                allow_lease_read: true,
            },
        );
        assert!(safe.safe);
        assert!(safe.lease_read);
    }

    #[test]
    fn learner_promotion_requires_health_and_low_lag() {
        let status = RustRaftStatusSnapshot {
            group_id: 1,
            node_id: 1,
            role: RustRaftRole::Leader,
            term: 4,
            leader_id: Some(1),
            commit_index: 20,
            applied_index: 20,
            last_log_index: 20,
            last_snapshot_index: 9,
            peers: vec![RustRaftPeerStatus {
                node_id: 3,
                matched: 19,
                next_index: 20,
                learner: true,
                healthy: true,
                lag: 1,
            }],
        };

        let ok = rustraft_learner_promotion_decision(&status, 3, 2);
        assert!(ok.promotable);
        assert_eq!(ok.reason, "learner_caught_up");

        let too_lagged = rustraft_learner_promotion_decision(&status, 3, 0);
        assert!(!too_lagged.promotable);
        assert_eq!(too_lagged.reason, "learner_lag_too_high");
    }

    #[test]
    fn append_safety_rejects_entries_before_snapshot_or_compaction_floor() {
        let safe_request = RustRaftAppendEntriesRequest {
            group_id: 1,
            term: 5,
            leader_id: 1,
            prev_log_id: Some(RustRaftLogId { term: 4, index: 10 }),
            entries: vec![RustRaftLogEntry {
                log_id: RustRaftLogId { term: 5, index: 11 },
                payload: b"write".to_vec(),
            }],
            leader_commit: 11,
        };
        assert!(rustraft_append_safety_decision(9, 10, &safe_request).accepted);

        let stale_prev = RustRaftAppendEntriesRequest {
            prev_log_id: Some(RustRaftLogId { term: 3, index: 8 }),
            ..safe_request.clone()
        };
        let decision = rustraft_append_safety_decision(9, 10, &stale_prev);
        assert!(!decision.accepted);
        assert_eq!(decision.reason, "prev_log_before_snapshot_floor");

        let compacted_entry = RustRaftAppendEntriesRequest {
            prev_log_id: Some(RustRaftLogId { term: 4, index: 10 }),
            entries: vec![RustRaftLogEntry {
                log_id: RustRaftLogId { term: 4, index: 9 },
                payload: b"old".to_vec(),
            }],
            ..safe_request
        };
        let decision = rustraft_append_safety_decision(8, 10, &compacted_entry);
        assert!(!decision.accepted);
        assert_eq!(decision.reason, "entry_compacted:9");
    }

    #[test]
    fn pipeline_evidence_requires_active_backpressure_and_reorder_observation() {
        let peers = vec![
            RustRaftPeerPipelineStatus {
                peer_id: 2,
                match_index: 10,
                next_index: 11,
                append_requests: 3,
                append_rejected: 1,
                inflight_entries: 1,
                inflight_bytes: 512,
                append_queue_depth: 1,
                append_queue_limit: 1,
                append_queue_max_depth: 1,
                inflight_bytes_limit: 512,
                memory_backpressure_rejections: 1,
                oversized_log_rejections: 1,
                out_of_order_append_rejections: 1,
                reorder_entries_rejected: 1,
                ..RustRaftPeerPipelineStatus::default()
            },
            RustRaftPeerPipelineStatus {
                peer_id: 3,
                match_index: 10,
                next_index: 11,
                append_requests: 2,
                apply_inflight_limit: 1,
                apply_queue_depth: 1,
                apply_queue_max_depth: 1,
                apply_batch_bytes_limit: 16,
                apply_backpressure_rejections: 1,
                ..RustRaftPeerPipelineStatus::default()
            },
        ];

        let evidence = rustraft_pipeline_evidence(
            &peers,
            RustRaftPipelineLimits {
                max_inflights_replicate: 1,
                max_memory_replicate_log_bytes: 512,
                max_inflights_apply_task: 1,
                max_apply_batch_bytes: 16,
                enable_reorder_queue: true,
                reorder_window_size: 4,
                reorder_timeout_us: 1_000,
            },
        );
        assert!(evidence.ready);
        assert!(evidence.append_backpressure_enforced);
        assert!(evidence.apply_backpressure_enforced);
        assert!(evidence.memory_replicate_bytes_enforced);
        assert!(evidence.oversized_log_rejection_present);
        assert!(evidence.out_of_order_append_handling_present);
    }

    #[test]
    fn snapshot_lifecycle_evidence_tracks_sender_downloader_and_rollback() {
        let peers = vec![RustRaftPeerPipelineStatus {
            peer_id: 2,
            next_index: 11,
            snapshot_sending: true,
            snapshot_installing: true,
            snapshot_installed_index: 10,
            snapshot_send_attempts: 2,
            snapshot_install_total_chunks: 4,
            snapshot_install_progress_per_mille: 750,
            snapshot_backpressure_rejections: 1,
            snapshot_rate_limit_rejections: 1,
            snapshot_install_rolled_back: 1,
            snapshot_during_membership_change: true,
            snapshot_rejoin_after_compacted_log: true,
            ..RustRaftPeerPipelineStatus::default()
        }];

        let evidence = rustraft_snapshot_lifecycle_evidence(&peers, 60_000, 2);
        assert!(evidence.ready);
        assert!(evidence.sender_lifecycle_present);
        assert!(evidence.downloader_lifecycle_present);
        assert!(evidence.retry_backpressure_present);
        assert!(evidence.install_rollback_present);
    }

    #[test]
    fn wal_lifecycle_evidence_exposes_segment_release_and_index_status() {
        let evidence = rustraft_wal_lifecycle_evidence(&RustRaftWalLifecycleStatus {
            segment_count: 2,
            active_segment_id: 2,
            first_retained_segment_id: 1,
            last_retained_segment_id: 2,
            total_bytes: 4096,
            active_segment_bytes: 1024,
            total_records: 7,
            first_sequence: 1,
            last_sequence: 7,
            first_log_index: 1,
            last_log_index: 7,
            released_segment_count: 1,
            slow_fsync_backpressure_observed: true,
        });
        assert!(evidence.ready);
        assert!(evidence.segment_lifecycle_present);
        assert!(evidence.release_rules_observed);
        assert!(evidence.first_last_index_status_present);
        assert!(evidence.slow_fsync_backpressure_observed);
    }
}
