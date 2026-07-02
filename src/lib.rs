#![forbid(unsafe_code)]
//! RustRaft is the TemporalStore-owned Raft contract and readiness library.
//!
//! The crate intentionally focuses on portable consensus-facing contracts:
//! request/response types, storage and transport traits, safety decisions,
//! metrics names, and fail-closed production readiness reports. It does not run
//! the TemporalStore data-node or metaserver by itself. Those runtimes consume
//! this crate and attach live evidence for pipeline, WAL, snapshot, membership,
//! failover, and process-rollout behavior.
//!
//! Typical integration flow:
//!
//! 1. Build a [`RustRaftReadinessSnapshot`] from the serving runtime.
//! 2. Call [`rustraft_parity_report`] for semantic contract readiness.
//! 3. Attach live runtime evidence to [`RustRaftProductionReadinessInput`].
//! 4. Call [`rustraft_production_readiness_report`] and block production claims
//!    unless the report is ready.
//!
//! The public API is OpenRaft-free by design. Compatibility with existing
//! TemporalStore deployment semantics is expressed through RustRaft-owned types
//! and tests instead of upstream-specific type aliases.
//! ByteRaft remains the feature and performance reference; RustRaft may expose
//! more idiomatic Rust traits and error types as long as TemporalStore consumes
//! it through a stable adapter boundary.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use thiserror::Error;

pub mod benchmark;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRequirementCategory {
    Safety,
    Durability,
    Observability,
    Transport,
    Membership,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSemanticRequirement {
    pub id: String,
    pub category: RustRaftRequirementCategory,
    pub readiness_field: String,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityContract {
    pub library_name: String,
    pub consensus_backend_boundary: String,
    pub openraft_dependency_removed: bool,
    pub requirements: Vec<RustRaftSemanticRequirement>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftProductionStatus {
    ProductionReady,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityReport {
    pub contract: RustRaftParityContract,
    pub byteraft_reference_policy: RustRaftByteRaftReferencePolicy,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub byteraft_parity_matrix: Vec<RustRaftByteRaftParityItem>,
    pub byteraft_gaps: Vec<String>,
    pub byteraft_intentional_differences: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftByteRaftReferencePolicy {
    pub feature_reference: String,
    pub performance_reference: String,
    pub rust_api_policy: String,
    pub temporalstore_consumption_boundary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftByteRaftParityStatus {
    Satisfied,
    Gap,
    IntentionalDifference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftByteRaftParityItem {
    pub id: String,
    pub required: bool,
    pub status: RustRaftByteRaftParityStatus,
    pub evidence: Vec<String>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProductionReadinessInput {
    pub readiness: RustRaftReadinessSnapshot,
    #[serde(default)]
    pub peer_pipeline: Option<RustRaftPipelineEvidence>,
    #[serde(default)]
    pub snapshot_lifecycle: Option<RustRaftSnapshotLifecycleEvidence>,
    #[serde(default)]
    pub wal_lifecycle: Option<RustRaftWalLifecycleEvidence>,
    #[serde(default)]
    pub data_node_rollout: Option<RustRaftDataNodeProcessRolloutReport>,
    #[serde(default)]
    pub metaserver_rollout: Option<RustRaftMetaProcessRolloutReport>,
    #[serde(default)]
    pub membership_transitions: Vec<RustRaftMembershipTransitionEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProductionReadinessReport {
    pub parity: RustRaftParityReport,
    pub public_api: RustRaftPublicApiContract,
    pub ready: bool,
    pub production_status: RustRaftProductionStatus,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub production_blockers: Vec<String>,
    pub recommended_next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadinessEvidence {
    pub requirement_id: String,
    pub readiness_field: String,
    pub present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPublicApiContract {
    pub storage_trait: String,
    pub transport_trait: String,
    pub rpc_messages: Vec<String>,
    pub safety_helpers: Vec<String>,
    pub metrics: RustRaftMetricNames,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetricNames {
    pub ready: String,
    pub append_latency_ms: String,
    pub vote_latency_ms: String,
    pub read_index_latency_ms: String,
    pub snapshot_install_latency_ms: String,
    pub peer_append_queue_depth: String,
    pub peer_reorder_queue_depth: String,
    pub peer_snapshot_installed_index: String,
    pub wal_segment_count: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerPipelineStatus {
    pub peer_id: u64,
    pub match_index: u64,
    pub next_index: u64,
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
    #[serde(default)]
    pub stale_term_rejections: u64,
    pub snapshot_sending: bool,
    pub snapshot_installing: bool,
    pub snapshot_installed_index: u64,
    pub snapshot_send_attempts: u64,
    pub snapshot_install_total_chunks: u64,
    pub snapshot_install_progress_per_mille: u64,
    pub snapshot_backpressure_rejections: u64,
    pub snapshot_rate_limit_rejections: u64,
    pub snapshot_install_rolled_back: u64,
    #[serde(default)]
    pub snapshot_chunk_retry_count: u64,
    #[serde(default)]
    pub snapshot_send_timeouts: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPipelineEvidence {
    pub per_peer_pipeline_state_present: bool,
    pub append_backpressure_enforced: bool,
    pub apply_backpressure_enforced: bool,
    pub memory_replicate_bytes_enforced: bool,
    pub oversized_log_rejection_present: bool,
    pub out_of_order_append_handling_present: bool,
    pub reorder_timeout_drop_present: bool,
    pub stale_term_rejection_present: bool,
    pub reorder_queue_enabled: bool,
}

pub type RaftPeerPipelineState = RustRaftPeerPipelineStatus;

impl RustRaftPipelineLimits {
    pub fn production_default() -> Self {
        Self {
            max_inflights_replicate: 256,
            max_memory_replicate_log_bytes: 64 * 1024 * 1024,
            max_inflights_apply_task: 1024,
            max_apply_batch_bytes: 8 * 1024 * 1024,
            enable_reorder_queue: true,
            reorder_window_size: 1024,
            reorder_timeout_us: 5_000_000,
        }
    }
}

impl Default for RustRaftPipelineLimits {
    fn default() -> Self {
        Self::production_default()
    }
}

impl RustRaftPeerPipelineStatus {
    pub fn new(
        peer_id: RustRaftNodeId,
        next_index: RustRaftLogIndex,
        limits: RustRaftPipelineLimits,
    ) -> Self {
        Self {
            peer_id,
            match_index: next_index.saturating_sub(1),
            next_index,
            append_requests: 0,
            append_accepted: 0,
            append_rejected: 0,
            inflight_entries: 0,
            inflight_bytes: 0,
            append_queue_depth: 0,
            append_queue_limit: limits.max_inflights_replicate,
            append_queue_max_depth: 0,
            inflight_bytes_limit: limits.max_memory_replicate_log_bytes,
            apply_inflight_tasks: 0,
            apply_inflight_limit: limits.max_inflights_apply_task,
            apply_queue_depth: 0,
            apply_queue_max_depth: 0,
            apply_batch_bytes_limit: limits.max_apply_batch_bytes,
            apply_backpressure_rejections: 0,
            memory_backpressure_rejections: 0,
            oversized_log_rejections: 0,
            reorder_queue_depth: 0,
            out_of_order_append_rejections: 0,
            reorder_entries_rejected: 0,
            reorder_entry_timeouts: 0,
            reorder_dropped_packages: 0,
            stale_term_rejections: 0,
            snapshot_sending: false,
            snapshot_installing: false,
            snapshot_installed_index: 0,
            snapshot_send_attempts: 0,
            snapshot_install_total_chunks: 0,
            snapshot_install_progress_per_mille: 0,
            snapshot_backpressure_rejections: 0,
            snapshot_rate_limit_rejections: 0,
            snapshot_install_rolled_back: 0,
            snapshot_chunk_retry_count: 0,
            snapshot_send_timeouts: 0,
            snapshot_during_membership_change: false,
            snapshot_rejoin_after_compacted_log: false,
            transfer_leader_target: false,
            transfer_leader_timeouts: 0,
            pre_vote_rejections: 0,
            election_rejections: 0,
            offline_timeout_reached: false,
            offline_timeout_rejections: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftInflightAppend {
    pub first_log_id: RustRaftLogId,
    pub last_log_id: RustRaftLogId,
    pub entry_count: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotTransferState {
    pub snapshot_id: RustRaftSnapshotId,
    pub snapshot_index: RustRaftLogIndex,
    pub total_chunks: u64,
    pub acknowledged_chunks: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicationPipeline {
    peer_id: RustRaftNodeId,
    limits: RustRaftPipelineLimits,
    status: RustRaftPeerPipelineStatus,
    append_queue: VecDeque<RustRaftLogEntry>,
    inflight: VecDeque<RaftInflightAppend>,
    reorder_queue: BTreeMap<RustRaftLogIndex, RustRaftLogEntry>,
    snapshot_transfer: Option<RaftSnapshotTransferState>,
}

impl RaftReplicationPipeline {
    pub fn new(
        peer_id: RustRaftNodeId,
        next_index: RustRaftLogIndex,
        limits: RustRaftPipelineLimits,
    ) -> Self {
        Self {
            peer_id,
            limits,
            status: RustRaftPeerPipelineStatus::new(peer_id, next_index, limits),
            append_queue: VecDeque::new(),
            inflight: VecDeque::new(),
            reorder_queue: BTreeMap::new(),
            snapshot_transfer: None,
        }
    }

    pub fn peer_id(&self) -> RustRaftNodeId {
        self.peer_id
    }

    pub fn status(&self) -> RustRaftPeerPipelineStatus {
        self.status.clone()
    }

    pub fn queue_append(&mut self, entry: RustRaftLogEntry) -> Result<(), RaftError> {
        let bytes = entry.payload.len() as u64;
        if bytes > self.limits.max_apply_batch_bytes {
            self.status.oversized_log_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append entry exceeds max apply batch bytes".to_string(),
            ));
        }
        if self.append_queue.len() as u64 >= self.status.append_queue_limit {
            self.status.apply_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append queue backpressure limit reached".to_string(),
            ));
        }
        if self.status.inflight_bytes + self.queued_bytes() + bytes
            > self.limits.max_memory_replicate_log_bytes
        {
            self.status.memory_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "replication memory backpressure limit reached".to_string(),
            ));
        }
        self.append_queue.push_back(entry);
        self.status.append_queue_depth = self.append_queue.len() as u64;
        self.status.append_queue_max_depth = self
            .status
            .append_queue_max_depth
            .max(self.status.append_queue_depth);
        Ok(())
    }

    pub fn flush_append_window(&mut self) -> Vec<RaftInflightAppend> {
        let mut flushed = Vec::new();
        while self.status.inflight_entries < self.limits.max_inflights_replicate {
            let Some(entry) = self.append_queue.pop_front() else {
                break;
            };
            let bytes = entry.payload.len() as u64;
            if self.status.inflight_bytes + bytes > self.limits.max_memory_replicate_log_bytes {
                self.append_queue.push_front(entry);
                self.status.memory_backpressure_rejections += 1;
                break;
            }
            let inflight = RaftInflightAppend {
                first_log_id: entry.log_id.clone(),
                last_log_id: entry.log_id,
                entry_count: 1,
                bytes,
            };
            self.status.append_requests += 1;
            self.status.inflight_entries += inflight.entry_count;
            self.status.inflight_bytes += inflight.bytes;
            self.inflight.push_back(inflight.clone());
            flushed.push(inflight);
        }
        self.status.append_queue_depth = self.append_queue.len() as u64;
        flushed
    }

    pub fn handle_append_response(
        &mut self,
        response: &RustRaftAppendEntriesResponse,
    ) -> Result<(), RaftError> {
        if response.term == 0 {
            self.status.stale_term_rejections += 1;
        }
        if response.success {
            self.status.append_accepted += 1;
            self.status.match_index = self.status.match_index.max(response.match_index);
            self.status.next_index = self.status.match_index + 1;
            self.release_inflight_through(response.match_index);
            self.drain_reorder_queue();
            Ok(())
        } else {
            self.status.append_rejected += 1;
            self.status.next_index = response
                .rejection_hint
                .unwrap_or(self.status.match_index)
                .saturating_add(1);
            Err(RaftError::InvalidRequest(
                "append rejected by peer pipeline".to_string(),
            ))
        }
    }

    pub fn receive_out_of_order(&mut self, entry: RustRaftLogEntry) -> Result<(), RaftError> {
        if entry.log_id.index < self.status.next_index {
            self.status.out_of_order_append_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "append is below peer next index".to_string(),
            ));
        }
        if entry.log_id.index == self.status.next_index {
            self.status.match_index = entry.log_id.index;
            self.status.next_index = entry.log_id.index + 1;
            self.drain_reorder_queue();
            return Ok(());
        }
        if !self.limits.enable_reorder_queue {
            self.status.out_of_order_append_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "out-of-order append received while reorder queue is disabled".to_string(),
            ));
        }
        if self.reorder_queue.len() as u64 >= self.limits.reorder_window_size {
            self.status.reorder_entries_rejected += 1;
            return Err(RaftError::InvalidRequest(
                "reorder queue window is full".to_string(),
            ));
        }
        self.reorder_queue.insert(entry.log_id.index, entry);
        self.status.reorder_queue_depth = self.reorder_queue.len() as u64;
        Ok(())
    }

    pub fn expire_reorder_queue(&mut self) -> u64 {
        let dropped = self.reorder_queue.len() as u64;
        if dropped > 0 {
            self.reorder_queue.clear();
            self.status.reorder_queue_depth = 0;
            self.status.reorder_entry_timeouts += dropped;
            self.status.reorder_dropped_packages += dropped;
        }
        dropped
    }

    pub fn begin_snapshot_send(
        &mut self,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        if self.status.snapshot_sending || self.status.snapshot_installing {
            self.status.snapshot_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is already active".to_string(),
            ));
        }
        self.status.snapshot_sending = true;
        self.status.snapshot_send_attempts += 1;
        self.status.snapshot_install_total_chunks = total_chunks;
        self.status.snapshot_install_progress_per_mille = 0;
        self.snapshot_transfer = Some(RaftSnapshotTransferState {
            snapshot_id: snapshot_id.into(),
            snapshot_index,
            total_chunks,
            acknowledged_chunks: 0,
            bytes_sent: 0,
            bytes_received: 0,
        });
        Ok(())
    }

    pub fn record_snapshot_chunk_sent(&mut self, bytes: u64) -> Result<(), RaftError> {
        let transfer = self
            .snapshot_transfer
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        transfer.bytes_sent += bytes;
        Ok(())
    }

    pub fn acknowledge_snapshot_chunk(&mut self) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.as_mut().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        transfer.acknowledged_chunks += 1;
        self.status.snapshot_install_progress_per_mille = if transfer.total_chunks == 0 {
            1000
        } else {
            (transfer.acknowledged_chunks * 1000 / transfer.total_chunks).min(1000)
        };
        if transfer.acknowledged_chunks >= transfer.total_chunks {
            self.finish_snapshot_install()?;
        }
        Ok(())
    }

    pub fn begin_snapshot_install(
        &mut self,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        if self.status.snapshot_sending || self.status.snapshot_installing {
            self.status.snapshot_backpressure_rejections += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot transfer is already active".to_string(),
            ));
        }
        self.status.snapshot_installing = true;
        self.status.snapshot_install_total_chunks = total_chunks;
        self.status.snapshot_install_progress_per_mille = 0;
        self.snapshot_transfer = Some(RaftSnapshotTransferState {
            snapshot_id: snapshot_id.into(),
            snapshot_index,
            total_chunks,
            acknowledged_chunks: 0,
            bytes_sent: 0,
            bytes_received: 0,
        });
        Ok(())
    }

    pub fn receive_snapshot_chunk(&mut self, bytes: u64, done: bool) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.as_mut().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot install is not active".to_string())
        })?;
        transfer.bytes_received += bytes;
        transfer.acknowledged_chunks += 1;
        self.status.snapshot_install_progress_per_mille = if transfer.total_chunks == 0 {
            1000
        } else {
            (transfer.acknowledged_chunks * 1000 / transfer.total_chunks).min(1000)
        };
        if done {
            self.finish_snapshot_install()?;
        }
        Ok(())
    }

    pub fn rollback_snapshot_install(&mut self) {
        self.snapshot_transfer = None;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_install_rolled_back += 1;
    }

    pub fn mark_snapshot_rejoin_after_compacted_log(&mut self) {
        self.status.snapshot_rejoin_after_compacted_log = true;
    }

    fn queued_bytes(&self) -> u64 {
        self.append_queue
            .iter()
            .map(|entry| entry.payload.len() as u64)
            .sum()
    }

    fn release_inflight_through(&mut self, match_index: RustRaftLogIndex) {
        while self
            .inflight
            .front()
            .map(|inflight| inflight.last_log_id.index <= match_index)
            .unwrap_or(false)
        {
            if let Some(inflight) = self.inflight.pop_front() {
                self.status.inflight_entries = self
                    .status
                    .inflight_entries
                    .saturating_sub(inflight.entry_count);
                self.status.inflight_bytes =
                    self.status.inflight_bytes.saturating_sub(inflight.bytes);
            }
        }
    }

    fn drain_reorder_queue(&mut self) {
        while let Some(entry) = self.reorder_queue.remove(&self.status.next_index) {
            self.status.match_index = entry.log_id.index;
            self.status.next_index = entry.log_id.index + 1;
        }
        self.status.reorder_queue_depth = self.reorder_queue.len() as u64;
    }

    fn finish_snapshot_install(&mut self) -> Result<(), RaftError> {
        let transfer = self.snapshot_transfer.take().ok_or_else(|| {
            RaftError::InvalidRequest("snapshot transfer is not active".to_string())
        })?;
        self.status.snapshot_sending = false;
        self.status.snapshot_installing = false;
        self.status.snapshot_installed_index = self
            .status
            .snapshot_installed_index
            .max(transfer.snapshot_index);
        self.status.snapshot_install_progress_per_mille = 1000;
        self.status.match_index = self.status.match_index.max(transfer.snapshot_index);
        self.status.next_index = self.status.match_index + 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftHealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReplicationHealth {
    pub status: RaftHealthStatus,
    pub leader_id: Option<RustRaftNodeId>,
    pub commit_index: RustRaftLogIndex,
    pub replicated_peer_count: u64,
    pub lagging_peer_count: u64,
    pub max_peer_lag: RustRaftLogIndex,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftApplyHealth {
    pub status: RaftHealthStatus,
    pub commit_index: RustRaftLogIndex,
    pub applied_index: RustRaftLogIndex,
    pub apply_lag: RustRaftLogIndex,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftCapabilityEvidence {
    pub capability: String,
    pub present: bool,
    pub evidence: Vec<String>,
    pub source_reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRuntimeLocalStatusReport {
    pub node_status: RustRaftStatusSnapshot,
    pub peer_pipeline: Vec<RaftPeerPipelineState>,
    pub replication_health: RaftReplicationHealth,
    pub apply_health: RaftApplyHealth,
    pub readiness: RustRaftReadinessSnapshot,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftClusterStatusReport {
    pub group_id: RustRaftGroupId,
    pub leader_id: Option<RustRaftNodeId>,
    pub nodes: Vec<RustRaftStatusSnapshot>,
    pub replication_health: RaftReplicationHealth,
    pub apply_health: RaftApplyHealth,
    pub ready: bool,
    pub health: RaftHealthStatus,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRuntimeAdminReport {
    pub cluster_status: RaftClusterStatusReport,
    pub readiness: RustRaftReadinessSnapshot,
    pub parity: RustRaftParityReport,
    pub public_api: RustRaftPublicApiContract,
    pub capability_evidence: Vec<RaftCapabilityEvidence>,
    pub ready: bool,
    pub health: RaftHealthStatus,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotLifecycleEvidence {
    pub sender_lifecycle_present: bool,
    pub downloader_lifecycle_present: bool,
    pub retry_backpressure_present: bool,
    pub chunk_retry_present: bool,
    pub send_timeout_present: bool,
    pub rate_limit_present: bool,
    pub install_progress_present: bool,
    pub install_rollback_present: bool,
    pub membership_change_present: bool,
    pub rejoin_after_compacted_log_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub first_log_index: u64,
    pub last_log_index: u64,
    pub released_segment_count: u64,
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalLifecycleEvidence {
    pub segment_lifecycle_present: bool,
    pub retained_range_present: bool,
    pub sequence_range_present: bool,
    pub log_index_range_present: bool,
    pub compaction_observed: bool,
    pub slow_fsync_backpressure_observed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProcessNodeEvidence {
    pub node_id: u64,
    pub addr: String,
    pub wal_dir: String,
    #[serde(default)]
    pub snapshot_dir: String,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_id: Option<String>,
    pub restarted: bool,
    pub log_store_validated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProcessOperationalSemanticsEvidence {
    #[serde(default)]
    pub api_presence_only_rejected: bool,
    #[serde(default)]
    pub process_path_validated: bool,
    #[serde(default)]
    pub read_index_validated: bool,
    #[serde(default)]
    pub leader_lease_validated: bool,
    #[serde(default)]
    pub stale_leader_lease_rejection_observed: bool,
    #[serde(default)]
    pub follower_lease_expiration_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub bounded_stale_read_acceptance_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_rejection_observed: bool,
    #[serde(default)]
    pub minority_partition_read_rejection_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_validated: bool,
    #[serde(default)]
    pub leader_transfer_under_load_validated: bool,
    #[serde(default)]
    pub snapshot_bootstrap_validated: bool,
    #[serde(default)]
    pub snapshot_install_restart_validated: bool,
    #[serde(default)]
    pub membership_rescale_validated: bool,
    #[serde(default)]
    pub membership_add_promote_remove_validated: bool,
    #[serde(default)]
    pub follower_rejoin_after_compaction_validated: bool,
    #[serde(default)]
    pub secondary_read_eligibility_validated: bool,
    #[serde(default)]
    pub apply_pipeline_converged: bool,
    #[serde(default)]
    pub wal_persistence_observed: bool,
    #[serde(default)]
    pub fsm_apply_idempotent_replay_observed: bool,
    #[serde(default)]
    pub storage_mutation_wal_fence_atomicity_observed: bool,
    #[serde(default)]
    pub snapshot_install_apply_fence_atomicity_observed: bool,
    #[serde(default)]
    pub process_restart_after_apply_crash_recovered: bool,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

impl RustRaftProcessOperationalSemanticsEvidence {
    pub fn proves_runtime_semantics(&self) -> bool {
        self.ready
            && self.blockers.is_empty()
            && self.api_presence_only_rejected
            && self.process_path_validated
            && self.read_index_validated
            && self.leader_lease_validated
            && self.stale_leader_lease_rejection_observed
            && self.follower_lease_expiration_observed
            && self.lagging_follower_read_rejected
            && self.bounded_stale_read_acceptance_observed
            && self.bounded_stale_read_rejection_observed
            && self.minority_partition_read_rejection_observed
            && self.healed_follower_catchup_observed
            && self.stale_follower_write_rejected
            && self.leader_transfer_exact_once_validated
            && self.leader_transfer_under_load_validated
            && self.snapshot_bootstrap_validated
            && self.snapshot_install_restart_validated
            && self.membership_rescale_validated
            && self.membership_add_promote_remove_validated
            && self.follower_rejoin_after_compaction_validated
            && self.secondary_read_eligibility_validated
            && self.apply_pipeline_converged
            && self.wal_persistence_observed
            && self.fsm_apply_idempotent_replay_observed
            && self.storage_mutation_wal_fence_atomicity_observed
            && self.snapshot_install_apply_fence_atomicity_observed
            && self.process_restart_after_apply_crash_recovered
    }

    pub fn missing_requirements(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for (present, requirement) in [
            (self.ready, "operational_semantics_ready"),
            (
                self.api_presence_only_rejected,
                "api_presence_only_rejected",
            ),
            (self.process_path_validated, "process_path_validated"),
            (self.read_index_validated, "read_index_validated"),
            (self.leader_lease_validated, "leader_lease_validated"),
            (
                self.stale_leader_lease_rejection_observed,
                "stale_leader_lease_rejection_observed",
            ),
            (
                self.follower_lease_expiration_observed,
                "follower_lease_expiration_observed",
            ),
            (
                self.lagging_follower_read_rejected,
                "lagging_follower_read_rejected",
            ),
            (
                self.bounded_stale_read_acceptance_observed,
                "bounded_stale_read_acceptance_observed",
            ),
            (
                self.bounded_stale_read_rejection_observed,
                "bounded_stale_read_rejection_observed",
            ),
            (
                self.minority_partition_read_rejection_observed,
                "minority_partition_read_rejection_observed",
            ),
            (
                self.healed_follower_catchup_observed,
                "healed_follower_catchup_observed",
            ),
            (
                self.stale_follower_write_rejected,
                "stale_follower_write_rejected",
            ),
            (
                self.leader_transfer_exact_once_validated,
                "leader_transfer_exact_once_validated",
            ),
            (
                self.leader_transfer_under_load_validated,
                "leader_transfer_under_load_validated",
            ),
            (
                self.snapshot_bootstrap_validated,
                "snapshot_bootstrap_validated",
            ),
            (
                self.snapshot_install_restart_validated,
                "snapshot_install_restart_validated",
            ),
            (
                self.membership_rescale_validated,
                "membership_rescale_validated",
            ),
            (
                self.membership_add_promote_remove_validated,
                "membership_add_promote_remove_validated",
            ),
            (
                self.follower_rejoin_after_compaction_validated,
                "follower_rejoin_after_compaction_validated",
            ),
            (
                self.secondary_read_eligibility_validated,
                "secondary_read_eligibility_validated",
            ),
            (self.apply_pipeline_converged, "apply_pipeline_converged"),
            (self.wal_persistence_observed, "wal_persistence_observed"),
            (
                self.fsm_apply_idempotent_replay_observed,
                "fsm_apply_idempotent_replay_observed",
            ),
            (
                self.storage_mutation_wal_fence_atomicity_observed,
                "storage_mutation_wal_fence_atomicity_observed",
            ),
            (
                self.snapshot_install_apply_fence_atomicity_observed,
                "snapshot_install_apply_fence_atomicity_observed",
            ),
            (
                self.process_restart_after_apply_crash_recovered,
                "process_restart_after_apply_crash_recovered",
            ),
        ] {
            if !present {
                missing.push(requirement.to_string());
            }
        }
        missing.extend(
            self.blockers
                .iter()
                .map(|blocker| format!("blocker:{blocker}")),
        );
        missing
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftDataNodeProcessRolloutReport {
    pub shard_id: u64,
    #[serde(default)]
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
    pub nodes: Vec<RustRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub write_proposed_through_process_api: bool,
    #[serde(default)]
    pub leader_transfer_validated: bool,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub secondary_lag_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejection_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejection_observed: bool,
    #[serde(default)]
    pub catchup_read_eligibility_observed: bool,
    #[serde(default)]
    pub minority_partition_rejection_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_eligibility_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub lagging_follower_observed_lag: u64,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub recovered_after_restart: bool,
    #[serde(default)]
    pub restart_recovery_validated: bool,
    pub snapshot_install_validated: bool,
    pub applied_fence_validated: bool,
    #[serde(default)]
    pub crash_after_storage_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_snapshot_install_recovered: bool,
    #[serde(default)]
    pub apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: RustRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMetaProcessRolloutReport {
    #[serde(default)]
    pub voters: Vec<u64>,
    #[serde(default)]
    pub learners: Vec<u64>,
    pub nodes: Vec<RustRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub mutation_proposed_through_process_api: bool,
    #[serde(default)]
    pub applied_raft_mutations: u64,
    #[serde(default)]
    pub generated_scheduler_tasks: u64,
    #[serde(default)]
    pub scheduler_retries: u64,
    #[serde(default)]
    pub stale_scheduler_token_rejected: bool,
    #[serde(default)]
    pub data_node_membership_results_ready: bool,
    #[serde(default)]
    pub scheduler_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub scheduler_task_replay_from_raft_log_observed: bool,
    #[serde(default)]
    pub membership_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub data_node_membership_workflow_report_attached: bool,
    #[serde(default)]
    pub data_node_raft_group_results_observed: bool,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub read_index_validated: bool,
    pub snapshot_install_validated: bool,
    pub recovered_after_restart: bool,
    pub scheduler_task_replay_validated: bool,
    #[serde(default)]
    pub crash_after_meta_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_meta_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_meta_snapshot_install_recovered: bool,
    #[serde(default)]
    pub meta_apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: RustRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftMembershipScope {
    Metaserver,
    DataNode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftMembershipTransitionKind {
    Failover,
    ScaleUp,
    ScaleDown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipTransitionEvidence {
    pub scope: RustRaftMembershipScope,
    pub transition: RustRaftMembershipTransitionKind,
    #[serde(default)]
    pub before_voters: Vec<u64>,
    #[serde(default)]
    pub after_voters: Vec<u64>,
    #[serde(default)]
    pub before_learners: Vec<u64>,
    #[serde(default)]
    pub after_learners: Vec<u64>,
    pub leader_before: Option<u64>,
    pub leader_after: Option<u64>,
    #[serde(default)]
    pub failed_or_removed_nodes: Vec<u64>,
    #[serde(default)]
    pub added_nodes: Vec<u64>,
    #[serde(default)]
    pub caught_up_nodes: Vec<u64>,
    pub commit_index_before: u64,
    pub commit_index_after: u64,
    pub applied_index_after: u64,
    pub joint_consensus_used: bool,
    pub old_majority_preserved: bool,
    pub new_majority_reached: bool,
    pub stale_leader_rejected: bool,
    pub read_index_validated_after: bool,
    pub write_validated_after: bool,
    pub snapshot_floor_preserved: bool,
    pub secondary_replication_visible: bool,
    #[serde(default)]
    pub scheduler_generation_advanced: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipTransitionDecision {
    pub scope: RustRaftMembershipScope,
    pub transition: RustRaftMembershipTransitionKind,
    pub ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembershipReadinessReport {
    pub ready: bool,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
    pub decisions: Vec<RustRaftMembershipTransitionDecision>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftRole {
    Leader,
    Follower,
    Candidate,
    Learner,
}

pub type RustRaftNodeId = u64;
pub type RustRaftGroupId = u64;
pub type RustRaftTerm = u64;
pub type RustRaftLogIndex = u64;
pub type RustRaftSnapshotId = String;
pub type RustRaftPayload = Vec<u8>;
pub type EntryPayload = RustRaftPayload;
pub type RustRaftSnapshotPayload = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLogId {
    pub term: RustRaftTerm,
    pub index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericLogEntry<P = RustRaftPayload> {
    pub log_id: RustRaftLogId,
    pub payload: P,
}

pub type RustRaftLogEntry = RustRaftGenericLogEntry<RustRaftPayload>;
pub type RaftLogEntry<P = EntryPayload> = RustRaftGenericLogEntry<P>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftHardState {
    pub current_term: RustRaftTerm,
    pub voted_for: Option<RustRaftNodeId>,
    pub committed: Option<RustRaftLogId>,
}

pub type RaftHardState = RustRaftHardState;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftReplicaRole {
    Voter,
    Learner,
    Witness,
}

impl RustRaftReplicaRole {
    pub fn participates_in_quorum(self) -> bool {
        matches!(self, Self::Voter | Self::Witness)
    }

    pub fn can_serve_data(self) -> bool {
        matches!(self, Self::Voter | Self::Learner)
    }

    pub fn can_be_leader(self) -> bool {
        matches!(self, Self::Voter)
    }
}

impl Default for RustRaftReplicaRole {
    fn default() -> Self {
        Self::Voter
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeer {
    pub node_id: RustRaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub role: RustRaftReplicaRole,
    #[serde(default)]
    pub auto_promote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftMembership {
    pub group_id: RustRaftGroupId,
    pub voters: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub learners: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub witnesses: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub epoch: u64,
}

pub type RaftMembership = RustRaftMembership;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftLearnerCatchUpReport {
    pub learner_id: RustRaftNodeId,
    pub learner_match_index: RustRaftLogIndex,
    pub leader_commit_index: RustRaftLogIndex,
    pub caught_up: bool,
    pub lag: RustRaftLogIndex,
    pub promotable: bool,
    pub reason: String,
}

impl RustRaftMembership {
    pub fn quorum_size(&self) -> usize {
        let participants = self.voters.len() + self.witnesses.len();
        participants / 2 + 1
    }

    pub fn quorum_reached<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        let acknowledgements: Vec<_> = acknowledgements.into_iter().collect();
        let votes = self
            .voters
            .iter()
            .chain(self.witnesses.iter())
            .filter(|node_id| acknowledgements.contains(node_id))
            .count();
        votes >= self.quorum_size()
    }

    pub fn add_learner(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        self.ensure_absent(node_id)?;
        self.learners.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn add_witness(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        self.ensure_absent(node_id)?;
        self.witnesses.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn promote_learner(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        let position = self
            .learners
            .iter()
            .position(|learner| *learner == node_id)
            .ok_or_else(|| {
                RaftError::InvalidRequest(format!("node {} is not a learner", node_id))
            })?;
        self.learners.remove(position);
        self.voters.push(node_id);
        self.epoch += 1;
        Ok(())
    }

    pub fn remove_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        let removed = remove_node(&mut self.voters, node_id)
            || remove_node(&mut self.learners, node_id)
            || remove_node(&mut self.witnesses, node_id);
        if !removed {
            return Err(RaftError::NodeNotFound(node_id));
        }
        self.epoch += 1;
        Ok(())
    }

    pub fn catchup_report(
        &self,
        learner_id: RustRaftNodeId,
        learner_match_index: RustRaftLogIndex,
        leader_commit_index: RustRaftLogIndex,
    ) -> RaftLearnerCatchUpReport {
        let lag = leader_commit_index.saturating_sub(learner_match_index);
        let is_learner = self.learners.contains(&learner_id);
        let caught_up = is_learner && learner_match_index >= leader_commit_index;
        RaftLearnerCatchUpReport {
            learner_id,
            learner_match_index,
            leader_commit_index,
            caught_up,
            lag,
            promotable: caught_up,
            reason: if !is_learner {
                "node_is_not_learner".to_string()
            } else if caught_up {
                "learner_caught_up".to_string()
            } else {
                "learner_lagging".to_string()
            },
        }
    }

    fn ensure_absent(&self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        if self.voters.contains(&node_id)
            || self.learners.contains(&node_id)
            || self.witnesses.contains(&node_id)
        {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is already a member",
                node_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftJointMembership {
    pub old_voters: Vec<RustRaftNodeId>,
    pub new_voters: Vec<RustRaftNodeId>,
}

pub type JointConsensusMembership = RustRaftJointMembership;

impl RustRaftJointMembership {
    pub fn old_quorum_size(&self) -> usize {
        self.old_voters.len() / 2 + 1
    }

    pub fn new_quorum_size(&self) -> usize {
        self.new_voters.len() / 2 + 1
    }

    pub fn quorum_reached<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        let acknowledgements: Vec<_> = acknowledgements.into_iter().collect();
        let old_votes = self
            .old_voters
            .iter()
            .filter(|node_id| acknowledgements.contains(node_id))
            .count();
        let new_votes = self
            .new_voters
            .iter()
            .filter(|node_id| acknowledgements.contains(node_id))
            .count();
        old_votes >= self.old_quorum_size() && new_votes >= self.new_quorum_size()
    }
}

fn remove_node(nodes: &mut Vec<RustRaftNodeId>, node_id: RustRaftNodeId) -> bool {
    if let Some(position) = nodes.iter().position(|existing| *existing == node_id) {
        nodes.remove(position);
        true
    } else {
        false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplySnapshotFence {
    pub applied_index: RustRaftLogIndex,
    pub commit_index: RustRaftLogIndex,
    pub installed_snapshot_index: RustRaftLogIndex,
    pub first_retained_log_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftWalRecord {
    pub group_id: RustRaftGroupId,
    pub node_id: RustRaftNodeId,
    pub hard_state: RustRaftHardState,
    pub membership: RustRaftMembership,
    #[serde(default)]
    pub entries: Vec<RustRaftLogEntry>,
    #[serde(default)]
    pub installed_snapshot: Option<RustRaftSnapshotMeta>,
    pub apply_snapshot_fence: RustRaftApplySnapshotFence,
    pub checksum: String,
}

pub type RaftWalRecord = RustRaftWalRecord;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalSegment {
    pub segment_id: u64,
    pub first_index: RustRaftLogIndex,
    pub last_index: RustRaftLogIndex,
    pub records: Vec<RaftWalRecord>,
    pub sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalRaftWal {
    pub max_records_per_segment: usize,
    segments: Vec<RaftWalSegment>,
    next_segment_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftWalRecoveryReport {
    pub recovered: Option<RaftWalRecord>,
    pub truncated_corrupt_tail: bool,
    pub surviving_records: usize,
    pub removed_records: usize,
}

impl LocalRaftWal {
    pub fn new(max_records_per_segment: usize) -> Result<Self, RaftError> {
        if max_records_per_segment == 0 {
            return Err(RaftError::InvalidRequest(
                "max_records_per_segment must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            max_records_per_segment,
            segments: vec![RaftWalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            }],
            next_segment_id: 1,
        })
    }

    pub fn append(&mut self, mut record: RaftWalRecord) -> Result<String, RaftError> {
        record.checksum = rustraft_wal_checksum(&record);
        if self
            .segments
            .last()
            .map(|segment| segment.records.len() >= self.max_records_per_segment)
            .unwrap_or(true)
        {
            if let Some(segment) = self.segments.last_mut() {
                segment.sealed = true;
            }
            self.segments.push(RaftWalSegment {
                segment_id: self.next_segment_id,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            });
            self.next_segment_id += 1;
        }

        let checksum = record.checksum.clone();
        let record_index = record
            .hard_state
            .committed
            .as_ref()
            .map(|log_id| log_id.index)
            .or_else(|| record.entries.last().map(|entry| entry.log_id.index))
            .unwrap_or_default();
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| RaftError::Storage("WAL has no active segment".to_string()))?;
        if segment.records.is_empty() {
            segment.first_index = record_index;
        }
        segment.last_index = record_index;
        segment.records.push(record);
        Ok(checksum)
    }

    pub fn segments(&self) -> &[RaftWalSegment] {
        &self.segments
    }

    pub fn records(&self) -> Vec<RaftWalRecord> {
        self.segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect()
    }

    pub fn recover(&mut self) -> Result<RaftWalRecoveryReport, RaftError> {
        let mut records = self.records();
        let original_len = records.len();
        while matches!(records.last(), Some(record) if !rustraft_wal_checksum_valid(record)) {
            records.pop();
        }
        let recovered = rustraft_recover_latest_wal_record(&records).ok();
        let truncated_corrupt_tail = records.len() != original_len;
        if truncated_corrupt_tail {
            self.rebuild_from_records(records.clone())?;
        }
        Ok(RaftWalRecoveryReport {
            recovered,
            truncated_corrupt_tail,
            surviving_records: records.len(),
            removed_records: original_len.saturating_sub(records.len()),
        })
    }

    pub fn corrupt_tail_for_test(&mut self) -> Result<(), RaftError> {
        let record = self
            .segments
            .last_mut()
            .and_then(|segment| segment.records.last_mut())
            .ok_or_else(|| RaftError::Storage("WAL has no tail record".to_string()))?;
        record.checksum = "corrupt-tail".to_string();
        Ok(())
    }

    fn rebuild_from_records(&mut self, records: Vec<RaftWalRecord>) -> Result<(), RaftError> {
        let max_records_per_segment = self.max_records_per_segment;
        *self = LocalRaftWal::new(max_records_per_segment)?;
        for record in records {
            self.append(record)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistentRaftWalOptions {
    pub dir: PathBuf,
    pub max_records_per_segment: usize,
    pub max_segment_bytes: u64,
    pub min_keep_segments: usize,
    pub fsync_on_append: bool,
}

impl PersistentRaftWalOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_records_per_segment: 10_000,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segments: 2,
            fsync_on_append: true,
        }
    }

    pub fn validate(&self) -> Result<(), RaftError> {
        if self.max_records_per_segment == 0 {
            return Err(RaftError::InvalidRequest(
                "max_records_per_segment must be greater than zero".to_string(),
            ));
        }
        if self.max_segment_bytes == 0 {
            return Err(RaftError::InvalidRequest(
                "max_segment_bytes must be greater than zero".to_string(),
            ));
        }
        if self.min_keep_segments == 0 {
            return Err(RaftError::InvalidRequest(
                "min_keep_segments must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PersistentRaftWal {
    options: PersistentRaftWalOptions,
    segments: Vec<RaftWalSegment>,
    active_segment: File,
    next_segment_id: u64,
    released_segment_count: u64,
    truncated_corrupt_tail: bool,
}

impl PersistentRaftWal {
    pub fn open(options: PersistentRaftWalOptions) -> Result<Self, RaftError> {
        options.validate()?;
        fs::create_dir_all(&options.dir).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create WAL directory {}: {err}",
                options.dir.display()
            ))
        })?;
        let (mut segments, truncated_corrupt_tail) = read_wal_segments_from_dir(&options.dir)?;
        if segments.is_empty() {
            segments.push(RaftWalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            });
            write_wal_segment_file(&options.dir, &segments[0])?;
        }
        let active_id = segments
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(0);
        for segment in segments.iter_mut() {
            segment.sealed = segment.segment_id != active_id;
        }
        let active_segment = open_segment_for_append(&options.dir, active_id)?;
        let next_segment_id = active_id + 1;
        Ok(Self {
            options,
            segments,
            active_segment,
            next_segment_id,
            released_segment_count: 0,
            truncated_corrupt_tail,
        })
    }

    pub fn append(&mut self, mut record: RaftWalRecord) -> Result<String, RaftError> {
        record.checksum = rustraft_wal_checksum(&record);
        let encoded = serde_json::to_string(&record)
            .map_err(|err| RaftError::Storage(format!("failed to encode WAL record: {err}")))?;
        let record_bytes = encoded.len() as u64 + 1;
        let active_len = self
            .active_segment
            .metadata()
            .map_err(|err| {
                RaftError::Storage(format!("failed to read WAL active segment metadata: {err}"))
            })?
            .len();
        let active_records = self
            .segments
            .last()
            .map(|segment| segment.records.len())
            .unwrap_or_default();
        if active_records >= self.options.max_records_per_segment
            || (active_records > 0 && active_len + record_bytes > self.options.max_segment_bytes)
        {
            self.roll_segment()?;
        }

        self.active_segment
            .write_all(encoded.as_bytes())
            .and_then(|_| self.active_segment.write_all(b"\n"))
            .map_err(|err| RaftError::Storage(format!("failed to append WAL record: {err}")))?;
        if self.options.fsync_on_append {
            self.active_segment
                .sync_data()
                .map_err(|err| RaftError::Storage(format!("failed to fsync WAL record: {err}")))?;
        }
        let checksum = record.checksum.clone();
        let record_index = wal_record_index(&record);
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| RaftError::Storage("WAL has no active segment".to_string()))?;
        if segment.records.is_empty() {
            segment.first_index = record_index;
        }
        segment.last_index = record_index;
        segment.records.push(record);
        Ok(checksum)
    }

    pub fn recover(&mut self) -> Result<RaftWalRecoveryReport, RaftError> {
        let (segments, truncated_corrupt_tail) = read_wal_segments_from_dir(&self.options.dir)?;
        let original_len = self.records().len();
        let records: Vec<_> = segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect();
        self.segments = if segments.is_empty() {
            vec![RaftWalSegment {
                segment_id: 0,
                first_index: 0,
                last_index: 0,
                records: Vec::new(),
                sealed: false,
            }]
        } else {
            segments
        };
        let active_id = self
            .segments
            .last()
            .map(|segment| segment.segment_id)
            .unwrap_or(0);
        for segment in self.segments.iter_mut() {
            segment.sealed = segment.segment_id != active_id;
        }
        self.active_segment = open_segment_for_append(&self.options.dir, active_id)?;
        self.next_segment_id = active_id + 1;
        let observed_corrupt_tail = self.truncated_corrupt_tail || truncated_corrupt_tail;
        self.truncated_corrupt_tail = observed_corrupt_tail;
        Ok(RaftWalRecoveryReport {
            recovered: rustraft_recover_latest_wal_record(&records).ok(),
            truncated_corrupt_tail: observed_corrupt_tail,
            surviving_records: records.len(),
            removed_records: original_len.saturating_sub(records.len()),
        })
    }

    pub fn compact_through(&mut self, log_index: RustRaftLogIndex) -> Result<u64, RaftError> {
        if self.segments.len() <= self.options.min_keep_segments {
            return Ok(0);
        }
        let removable_count = self
            .segments
            .len()
            .saturating_sub(self.options.min_keep_segments);
        let removable_ids: Vec<_> = self
            .segments
            .iter()
            .take(removable_count)
            .filter(|segment| segment.last_index > 0 && segment.last_index <= log_index)
            .map(|segment| segment.segment_id)
            .collect();
        for segment_id in &removable_ids {
            let path = wal_segment_path(&self.options.dir, *segment_id);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(RaftError::Storage(format!(
                        "failed to remove compacted WAL segment {}: {err}",
                        path.display()
                    )));
                }
            }
        }
        if !removable_ids.is_empty() {
            self.segments
                .retain(|segment| !removable_ids.contains(&segment.segment_id));
            self.released_segment_count += removable_ids.len() as u64;
        }
        Ok(removable_ids.len() as u64)
    }

    pub fn status(&self) -> RustRaftWalLifecycleStatus {
        let total_records = self.records().len() as u64;
        let total_bytes = self
            .segments
            .iter()
            .map(|segment| {
                fs::metadata(wal_segment_path(&self.options.dir, segment.segment_id))
                    .map(|metadata| metadata.len())
                    .unwrap_or_default()
            })
            .sum();
        let active_segment_bytes = self
            .segments
            .last()
            .and_then(|segment| {
                fs::metadata(wal_segment_path(&self.options.dir, segment.segment_id)).ok()
            })
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        RustRaftWalLifecycleStatus {
            segment_count: self.segments.len() as u64,
            active_segment_id: self
                .segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            first_retained_segment_id: self
                .segments
                .first()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            last_retained_segment_id: self
                .segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            total_bytes,
            active_segment_bytes,
            total_records,
            first_sequence: self
                .segments
                .first()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            last_sequence: self
                .segments
                .last()
                .map(|segment| segment.segment_id)
                .unwrap_or(0),
            first_log_index: self
                .segments
                .first()
                .map(|segment| segment.first_index)
                .unwrap_or(0),
            last_log_index: self
                .segments
                .last()
                .map(|segment| segment.last_index)
                .unwrap_or(0),
            released_segment_count: self.released_segment_count,
            slow_fsync_backpressure_observed: false,
        }
    }

    pub fn segments(&self) -> &[RaftWalSegment] {
        &self.segments
    }

    pub fn records(&self) -> Vec<RaftWalRecord> {
        self.segments
            .iter()
            .flat_map(|segment| segment.records.iter().cloned())
            .collect()
    }

    pub fn corrupt_tail_for_test(&mut self) -> Result<(), RaftError> {
        self.active_segment
            .seek(SeekFrom::End(0))
            .and_then(|_| self.active_segment.write_all(b"{\"corrupt_tail\":true\n"))
            .and_then(|_| self.active_segment.flush())
            .map_err(|err| RaftError::Storage(format!("failed to corrupt WAL tail: {err}")))?;
        Ok(())
    }

    fn roll_segment(&mut self) -> Result<(), RaftError> {
        if let Some(segment) = self.segments.last_mut() {
            segment.sealed = true;
            write_wal_segment_file(&self.options.dir, segment)?;
        }
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;
        let segment = RaftWalSegment {
            segment_id,
            first_index: 0,
            last_index: 0,
            records: Vec::new(),
            sealed: false,
        };
        write_wal_segment_file(&self.options.dir, &segment)?;
        self.active_segment = open_segment_for_append(&self.options.dir, segment_id)?;
        self.segments.push(segment);
        Ok(())
    }
}

pub type FileRaftWal = PersistentRaftWal;

fn wal_segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:020}.wal"))
}

fn wal_record_index(record: &RaftWalRecord) -> RustRaftLogIndex {
    record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .or_else(|| record.entries.last().map(|entry| entry.log_id.index))
        .unwrap_or_default()
}

fn open_segment_for_append(dir: &Path, segment_id: u64) -> Result<File, RaftError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(wal_segment_path(dir, segment_id))
        .map_err(|err| RaftError::Storage(format!("failed to open WAL segment: {err}")))
}

fn write_wal_segment_file(dir: &Path, segment: &RaftWalSegment) -> Result<(), RaftError> {
    let mut file = File::create(wal_segment_path(dir, segment.segment_id))
        .map_err(|err| RaftError::Storage(format!("failed to create WAL segment: {err}")))?;
    for record in &segment.records {
        let encoded = serde_json::to_string(record)
            .map_err(|err| RaftError::Storage(format!("failed to encode WAL segment: {err}")))?;
        file.write_all(encoded.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|err| RaftError::Storage(format!("failed to write WAL segment: {err}")))?;
    }
    file.sync_data()
        .map_err(|err| RaftError::Storage(format!("failed to fsync WAL segment: {err}")))
}

fn read_wal_segments_from_dir(dir: &Path) -> Result<(Vec<RaftWalSegment>, bool), RaftError> {
    let mut files = fs::read_dir(dir)
        .map_err(|err| RaftError::Storage(format!("failed to read WAL directory: {err}")))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let segment_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())?;
            (path.extension().and_then(|ext| ext.to_str()) == Some("wal"))
                .then_some((segment_id, path))
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|(segment_id, _)| *segment_id);

    let mut segments = Vec::new();
    let mut truncated_corrupt_tail = false;
    let last_file_index = files.len().saturating_sub(1);
    for (file_position, (segment_id, path)) in files.into_iter().enumerate() {
        let (records, truncated) = read_wal_segment_file(&path)?;
        truncated_corrupt_tail |= truncated;
        let first_index = records.first().map(wal_record_index).unwrap_or_default();
        let last_index = records.last().map(wal_record_index).unwrap_or_default();
        segments.push(RaftWalSegment {
            segment_id,
            first_index,
            last_index,
            records,
            sealed: file_position != last_file_index,
        });
    }
    Ok((segments, truncated_corrupt_tail))
}

fn read_wal_segment_file(path: &Path) -> Result<(Vec<RaftWalRecord>, bool), RaftError> {
    let file = File::open(path)
        .map_err(|err| RaftError::Storage(format!("failed to open WAL segment: {err}")))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut valid_end_offset = 0u64;
    let mut current_offset = 0u64;
    let mut truncated = false;
    for line in reader.split(b'\n') {
        let line = line.map_err(|err| {
            RaftError::Storage(format!(
                "failed to read WAL segment {}: {err}",
                path.display()
            ))
        })?;
        current_offset += line.len() as u64 + 1;
        if line.is_empty() {
            valid_end_offset = current_offset;
            continue;
        }
        let Ok(record) = serde_json::from_slice::<RaftWalRecord>(&line) else {
            truncated = true;
            break;
        };
        if !rustraft_wal_checksum_valid(&record) {
            truncated = true;
            break;
        }
        valid_end_offset = current_offset;
        records.push(record);
    }
    if truncated {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|err| RaftError::Storage(format!("failed to reopen WAL segment: {err}")))?;
        file.set_len(valid_end_offset)
            .map_err(|err| RaftError::Storage(format!("failed to truncate WAL segment: {err}")))?;
        file.sync_data()
            .map_err(|err| RaftError::Storage(format!("failed to fsync truncated WAL: {err}")))?;
    }
    Ok((records, truncated))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftConfig {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub leader_lease_ms: u64,
    pub max_payload_bytes: u64,
    pub snapshot_threshold_entries: u64,
    pub max_segment_bytes: u64,
    pub min_keep_segment_num: u64,
    pub enable_pre_vote: bool,
    pub enable_lease_read: bool,
}

impl Default for RustRaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: 1_000,
            heartbeat_interval_ms: 100,
            leader_lease_ms: 500,
            max_payload_bytes: 8 * 1024 * 1024,
            snapshot_threshold_entries: 10_000,
            max_segment_bytes: 64 * 1024 * 1024,
            min_keep_segment_num: 2,
            enable_pre_vote: true,
            enable_lease_read: true,
        }
    }
}

impl RustRaftConfig {
    pub fn validate(&self) -> Result<(), RaftConfigError> {
        if self.election_timeout_ms == 0 {
            return Err(RaftConfigError::ZeroElectionTimeout);
        }
        if self.heartbeat_interval_ms == 0 {
            return Err(RaftConfigError::ZeroHeartbeatInterval);
        }
        if self.leader_lease_ms == 0 {
            return Err(RaftConfigError::ZeroLeaderLease);
        }
        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            return Err(RaftConfigError::HeartbeatNotLessThanElection {
                heartbeat_interval_ms: self.heartbeat_interval_ms,
                election_timeout_ms: self.election_timeout_ms,
            });
        }
        if self.leader_lease_ms >= self.election_timeout_ms {
            return Err(RaftConfigError::LeaseNotLessThanElection {
                leader_lease_ms: self.leader_lease_ms,
                election_timeout_ms: self.election_timeout_ms,
            });
        }
        if self.max_payload_bytes == 0 {
            return Err(RaftConfigError::ZeroMaxPayloadBytes);
        }
        if self.snapshot_threshold_entries == 0 {
            return Err(RaftConfigError::ZeroSnapshotThreshold);
        }
        if self.max_segment_bytes == 0 {
            return Err(RaftConfigError::ZeroMaxSegmentBytes);
        }
        Ok(())
    }
}

pub type RaftConfig = RustRaftConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum RaftConfigError {
    #[error("election_timeout_ms must be greater than zero")]
    ZeroElectionTimeout,
    #[error("heartbeat_interval_ms must be greater than zero")]
    ZeroHeartbeatInterval,
    #[error("leader_lease_ms must be greater than zero")]
    ZeroLeaderLease,
    #[error(
        "heartbeat_interval_ms ({heartbeat_interval_ms}) must be less than election_timeout_ms ({election_timeout_ms})"
    )]
    HeartbeatNotLessThanElection {
        heartbeat_interval_ms: u64,
        election_timeout_ms: u64,
    },
    #[error(
        "leader_lease_ms ({leader_lease_ms}) must be less than election_timeout_ms ({election_timeout_ms})"
    )]
    LeaseNotLessThanElection {
        leader_lease_ms: u64,
        election_timeout_ms: u64,
    },
    #[error("max_payload_bytes must be greater than zero")]
    ZeroMaxPayloadBytes,
    #[error("snapshot_threshold_entries must be greater than zero")]
    ZeroSnapshotThreshold,
    #[error("max_segment_bytes must be greater than zero")]
    ZeroMaxSegmentBytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftNodeOptions {
    pub group_id: RustRaftGroupId,
    pub node_id: RustRaftNodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub wal_dir: String,
    pub snapshot_dir: String,
    pub role: RustRaftReplicaRole,
    pub config: RustRaftConfig,
    #[serde(default)]
    pub peers: Vec<RustRaftPeer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftProposeOptions {
    pub expected_term: Option<RustRaftTerm>,
    pub is_command: bool,
}

impl Default for RustRaftProposeOptions {
    fn default() -> Self {
        Self {
            expected_term: None,
            is_command: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplyRequest {
    pub group_id: RustRaftGroupId,
    pub log_id: RustRaftLogId,
    pub payload: RustRaftPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftApplyResponse {
    pub applied_index: RustRaftLogIndex,
    pub response: RustRaftPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericApplyRequest<G = RustRaftGroupId, P = RustRaftPayload> {
    pub group_id: G,
    pub log_id: RustRaftLogId,
    pub payload: P,
}

pub type RaftApplyRequest<G = RustRaftGroupId, P = EntryPayload> =
    RustRaftGenericApplyRequest<G, P>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericApplyResponse<P = RustRaftPayload> {
    pub applied_index: RustRaftLogIndex,
    pub response: P,
}

pub type RaftApplyResponse<P = EntryPayload> = RustRaftGenericApplyResponse<P>;

pub trait RaftApply<G = RustRaftGroupId, P = EntryPayload> {
    type Response;

    fn apply(
        &mut self,
        request: RaftApplyRequest<G, P>,
    ) -> Result<RaftApplyResponse<Self::Response>, RaftError>;
}

pub trait RaftStateMachine<G = RustRaftGroupId, P = EntryPayload>: RaftApply<G, P> {
    type Snapshot;

    fn snapshot(&self, group_id: G) -> Result<Self::Snapshot, RaftError>;
    fn install_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), RaftError>;
}

pub fn rustraft_apply_entry<S, G, P>(
    state_machine: &mut S,
    group_id: G,
    entry: RaftLogEntry<P>,
) -> Result<RaftApplyResponse<S::Response>, RaftError>
where
    S: RaftApply<G, P>,
{
    state_machine.apply(RaftApplyRequest {
        group_id,
        log_id: entry.log_id,
        payload: entry.payload,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmApplyOutcome<R> {
    pub response: RaftApplyResponse<R>,
    pub applied: bool,
    pub replayed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmReplayReport {
    pub attempted: u64,
    pub applied: u64,
    pub skipped_replay: u64,
    pub last_applied: RustRaftLogIndex,
    pub idempotent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmCheckpoint<G, S> {
    pub group_id: G,
    pub last_applied: RustRaftLogIndex,
    pub applied_log_ids: Vec<RustRaftLogId>,
    pub snapshot: S,
}

#[derive(Debug, Clone)]
pub struct RaftFsmAdapter<S, G = RustRaftGroupId, P = EntryPayload>
where
    S: RaftStateMachine<G, P>,
{
    group_id: G,
    state_machine: S,
    applied: BTreeMap<RustRaftLogIndex, RustRaftTerm>,
    responses: BTreeMap<RustRaftLogIndex, RaftApplyResponse<S::Response>>,
    last_applied: RustRaftLogIndex,
    _payload: PhantomData<P>,
}

impl<S, G, P> RaftFsmAdapter<S, G, P>
where
    S: RaftStateMachine<G, P>,
    G: Clone,
    P: Clone,
    S::Response: Clone,
{
    pub fn new(group_id: G, state_machine: S) -> Self {
        Self {
            group_id,
            state_machine,
            applied: BTreeMap::new(),
            responses: BTreeMap::new(),
            last_applied: 0,
            _payload: PhantomData,
        }
    }

    pub fn apply_entry(
        &mut self,
        entry: RaftLogEntry<P>,
    ) -> Result<RaftFsmApplyOutcome<S::Response>, RaftError> {
        if let Some(term) = self.applied.get(&entry.log_id.index) {
            if *term != entry.log_id.term {
                return Err(RaftError::InvalidRequest(format!(
                    "FSM replay conflict at index {}: existing term {}, replay term {}",
                    entry.log_id.index, term, entry.log_id.term
                )));
            }
            let response = self
                .responses
                .get(&entry.log_id.index)
                .cloned()
                .ok_or_else(|| {
                    RaftError::Storage(format!(
                        "FSM replay response missing for applied index {}",
                        entry.log_id.index
                    ))
                })?;
            return Ok(RaftFsmApplyOutcome {
                response,
                applied: false,
                replayed: true,
                reason: "duplicate_log_id_replayed_idempotently".to_string(),
            });
        }

        let response = self.state_machine.apply(RaftApplyRequest {
            group_id: self.group_id.clone(),
            log_id: entry.log_id.clone(),
            payload: entry.payload,
        })?;
        self.last_applied = self.last_applied.max(response.applied_index);
        self.applied.insert(entry.log_id.index, entry.log_id.term);
        self.responses.insert(entry.log_id.index, response.clone());
        Ok(RaftFsmApplyOutcome {
            response,
            applied: true,
            replayed: false,
            reason: "applied_new_log_id".to_string(),
        })
    }

    pub fn replay_entries<I>(&mut self, entries: I) -> Result<RaftFsmReplayReport, RaftError>
    where
        I: IntoIterator<Item = RaftLogEntry<P>>,
    {
        let mut report = RaftFsmReplayReport {
            attempted: 0,
            applied: 0,
            skipped_replay: 0,
            last_applied: self.last_applied,
            idempotent: true,
        };
        for entry in entries {
            report.attempted += 1;
            let outcome = self.apply_entry(entry)?;
            if outcome.applied {
                report.applied += 1;
            }
            if outcome.replayed {
                report.skipped_replay += 1;
            }
        }
        report.last_applied = self.last_applied;
        Ok(report)
    }

    pub fn checkpoint(&self) -> Result<RaftFsmCheckpoint<G, S::Snapshot>, RaftError> {
        Ok(RaftFsmCheckpoint {
            group_id: self.group_id.clone(),
            last_applied: self.last_applied,
            applied_log_ids: self
                .applied
                .iter()
                .map(|(index, term)| RustRaftLogId {
                    term: *term,
                    index: *index,
                })
                .collect(),
            snapshot: self.state_machine.snapshot(self.group_id.clone())?,
        })
    }

    pub fn install_checkpoint(
        &mut self,
        checkpoint: RaftFsmCheckpoint<G, S::Snapshot>,
    ) -> Result<(), RaftError> {
        self.state_machine.install_snapshot(checkpoint.snapshot)?;
        self.last_applied = checkpoint.last_applied;
        self.applied = checkpoint
            .applied_log_ids
            .into_iter()
            .map(|log_id| (log_id.index, log_id.term))
            .collect();
        self.responses.clear();
        Ok(())
    }

    pub fn last_applied(&self) -> RustRaftLogIndex {
        self.last_applied
    }

    pub fn applied_log_count(&self) -> usize {
        self.applied.len()
    }

    pub fn inner(&self) -> &S {
        &self.state_machine
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.state_machine
    }
}

pub trait RustRaftStateMachine {
    fn apply(
        &mut self,
        request: RustRaftApplyRequest,
    ) -> Result<RustRaftApplyResponse, RustRaftError>;
    fn snapshot(&self) -> Result<RustRaftSnapshotChunk, RustRaftError>;
    fn install_snapshot(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RustRaftError>;
}

impl<T> RaftApply<RustRaftGroupId, EntryPayload> for T
where
    T: RustRaftStateMachine,
{
    type Response = EntryPayload;

    fn apply(
        &mut self,
        request: RaftApplyRequest<RustRaftGroupId, EntryPayload>,
    ) -> Result<RaftApplyResponse<Self::Response>, RaftError> {
        let response = RustRaftStateMachine::apply(
            self,
            RustRaftApplyRequest {
                group_id: request.group_id,
                log_id: request.log_id,
                payload: request.payload,
            },
        )?;
        Ok(RaftApplyResponse {
            applied_index: response.applied_index,
            response: response.response,
        })
    }
}

pub trait RustRaftConsensus {
    fn start(&mut self) -> Result<(), RustRaftError>;
    fn stop(&mut self) -> Result<(), RustRaftError>;
    fn status(&self) -> Result<RustRaftStatusSnapshot, RustRaftError>;
    fn propose(
        &mut self,
        payload: RustRaftPayload,
        options: RustRaftProposeOptions,
    ) -> Result<RustRaftLogId, RustRaftError>;
    fn read_index(
        &self,
        min_commit_index: RustRaftLogIndex,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError>;
    fn add_peer(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError>;
    fn add_learner(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError>;
    fn promote_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError>;
    fn add_witness(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError>;
    fn remove_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError>;
    fn transfer_leader(&mut self, target: RustRaftNodeId) -> Result<(), RustRaftError>;
    fn campaign(&mut self, forced: bool) -> Result<(), RustRaftError>;
    fn trigger_snapshot(&mut self) -> Result<RustRaftSnapshotMeta, RustRaftError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftByteRaftParitySurface {
    pub node_lifecycle: Vec<String>,
    pub write_api: Vec<String>,
    pub read_api: Vec<String>,
    pub membership_api: Vec<String>,
    pub durability_api: Vec<String>,
    pub observability_api: Vec<String>,
}

pub fn rustraft_byteraft_parity_surface() -> RustRaftByteRaftParitySurface {
    RustRaftByteRaftParitySurface {
        node_lifecycle: vec![
            "create".to_string(),
            "start".to_string(),
            "restart".to_string(),
            "stop".to_string(),
            "shutdown".to_string(),
        ],
        write_api: vec![
            "propose".to_string(),
            "propose_options.expected_term".to_string(),
        ],
        read_api: vec!["read_index".to_string(), "lease_read".to_string()],
        membership_api: vec![
            "add_node".to_string(),
            "add_learner".to_string(),
            "add_witness".to_string(),
            "promote".to_string(),
            "remove_node".to_string(),
            "transfer_leader".to_string(),
            "campaign".to_string(),
        ],
        durability_api: vec![
            "wal_hard_state".to_string(),
            "snapshot_install".to_string(),
            "snapshot_tail_catchup".to_string(),
            "apply_snapshot_fence".to_string(),
        ],
        observability_api: vec![
            "status".to_string(),
            "local_status".to_string(),
            "metrics".to_string(),
            "fatal_events".to_string(),
        ],
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftPeerStatus {
    pub node_id: RustRaftNodeId,
    pub matched: RustRaftLogIndex,
    pub next_index: RustRaftLogIndex,
    pub learner: bool,
    pub healthy: bool,
    pub lag: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftStatusSnapshot {
    pub group_id: RustRaftGroupId,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesRequest {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub prev_log_id: Option<RustRaftLogId>,
    pub entries: Vec<RustRaftLogEntry>,
    pub leader_commit: RustRaftLogIndex,
}

pub type AppendEntriesRequest = RustRaftAppendEntriesRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendEntriesResponse {
    pub term: RustRaftTerm,
    pub success: bool,
    pub match_index: RustRaftLogIndex,
    pub rejection_hint: Option<RustRaftLogIndex>,
}

pub type AppendEntriesResponse = RustRaftAppendEntriesResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteRequest {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub candidate_id: RustRaftNodeId,
    pub last_log_id: Option<RustRaftLogId>,
    pub pre_vote: bool,
}

pub type VoteRequest = RustRaftVoteRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftVoteResponse {
    pub term: RustRaftTerm,
    pub vote_granted: bool,
    pub reason: String,
}

pub type VoteResponse = RustRaftVoteResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotMeta {
    pub snapshot_id: RustRaftSnapshotId,
    pub last_log_id: RustRaftLogId,
    pub membership: Vec<RustRaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotChunk {
    pub meta: RustRaftSnapshotMeta,
    pub offset: u64,
    pub data: RustRaftSnapshotPayload,
    pub done: bool,
}

pub type SnapshotChunk = RustRaftSnapshotChunk;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericSnapshot<G = RustRaftGroupId, P = RustRaftSnapshotPayload> {
    pub group_id: G,
    pub meta: RustRaftSnapshotMeta,
    pub payload: P,
}

pub type RaftSnapshot = RustRaftGenericSnapshot<RustRaftGroupId, RustRaftSnapshotPayload>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericSnapshotChunk<P = RustRaftSnapshotPayload> {
    pub meta: RustRaftSnapshotMeta,
    pub offset: u64,
    pub data: P,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotInstallState {
    pub meta: RustRaftSnapshotMeta,
    pub bytes: RustRaftSnapshotPayload,
    pub next_offset: u64,
    pub complete: bool,
}

impl RaftSnapshotInstallState {
    pub fn new(meta: RustRaftSnapshotMeta) -> Self {
        Self {
            meta,
            bytes: Vec::new(),
            next_offset: 0,
            complete: false,
        }
    }

    pub fn install_chunk(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RaftError> {
        if chunk.meta != self.meta {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk metadata changed during install".to_string(),
            ));
        }
        if chunk.offset != self.next_offset {
            return Err(RaftError::InvalidRequest(format!(
                "snapshot chunk offset {} does not match next offset {}",
                chunk.offset, self.next_offset
            )));
        }
        self.next_offset += chunk.data.len() as u64;
        self.bytes.extend_from_slice(&chunk.data);
        self.complete = chunk.done;
        Ok(())
    }

    pub fn finish(self, group_id: RustRaftGroupId) -> Result<RaftSnapshot, RaftError> {
        if !self.complete {
            return Err(RaftError::InvalidRequest(
                "snapshot install is incomplete".to_string(),
            ));
        }
        Ok(RaftSnapshot {
            group_id,
            meta: self.meta,
            payload: self.bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotLifecycleConfig {
    pub chunk_size: u64,
    pub max_chunks_per_tick: u64,
    pub max_bytes_per_tick: u64,
    pub max_retry_attempts: u64,
}

impl Default for RaftSnapshotLifecycleConfig {
    fn default() -> Self {
        Self {
            chunk_size: 64 * 1024,
            max_chunks_per_tick: 16,
            max_bytes_per_tick: 4 * 1024 * 1024,
            max_retry_attempts: 3,
        }
    }
}

impl RaftSnapshotLifecycleConfig {
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk_size must be greater than zero".to_string(),
            ));
        }
        if self.max_chunks_per_tick == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot max_chunks_per_tick must be greater than zero".to_string(),
            ));
        }
        if self.max_bytes_per_tick == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot max_bytes_per_tick must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotLifecycleStatus {
    pub snapshot_id: Option<RustRaftSnapshotId>,
    pub sending: bool,
    pub installing: bool,
    pub total_chunks: u64,
    pub sent_chunks: u64,
    pub received_chunks: u64,
    pub retry_count: u64,
    pub throttled_ticks: u64,
    pub rolled_back: u64,
    pub completed: bool,
    pub installed_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotSendState {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub chunks: Vec<RustRaftSnapshotChunk>,
    pub next_chunk: usize,
    pub accepted_chunks: u64,
    pub retry_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotLifecycle {
    config: RaftSnapshotLifecycleConfig,
    sender: Option<RaftSnapshotSendState>,
    installer: Option<RaftSnapshotInstallState>,
    status: RaftSnapshotLifecycleStatus,
}

impl RaftSnapshotLifecycle {
    pub fn new(config: RaftSnapshotLifecycleConfig) -> Result<Self, RaftError> {
        config.validate()?;
        Ok(Self {
            config,
            sender: None,
            installer: None,
            status: RaftSnapshotLifecycleStatus {
                snapshot_id: None,
                sending: false,
                installing: false,
                total_chunks: 0,
                sent_chunks: 0,
                received_chunks: 0,
                retry_count: 0,
                throttled_ticks: 0,
                rolled_back: 0,
                completed: false,
                installed_index: 0,
            },
        })
    }

    pub fn checkpoint(
        snapshot: &RaftSnapshot,
        chunk_size: u64,
    ) -> Result<Vec<RustRaftSnapshotChunk>, RaftError> {
        if chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk_size must be greater than zero".to_string(),
            ));
        }
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < snapshot.payload.len() {
            let end = (offset + chunk_size as usize).min(snapshot.payload.len());
            chunks.push(RustRaftSnapshotChunk {
                meta: snapshot.meta.clone(),
                offset: offset as u64,
                data: snapshot.payload[offset..end].to_vec(),
                done: end == snapshot.payload.len(),
            });
            offset = end;
        }
        if chunks.is_empty() {
            chunks.push(RustRaftSnapshotChunk {
                meta: snapshot.meta.clone(),
                offset: 0,
                data: Vec::new(),
                done: true,
            });
        }
        Ok(chunks)
    }

    pub fn begin_send(
        &mut self,
        snapshot: &RaftSnapshot,
        term: RustRaftTerm,
        leader_id: RustRaftNodeId,
    ) -> Result<(), RaftError> {
        if self.sender.is_some() || self.installer.is_some() {
            return Err(RaftError::InvalidRequest(
                "snapshot lifecycle is already active".to_string(),
            ));
        }
        let chunks = Self::checkpoint(snapshot, self.config.chunk_size)?;
        self.status = RaftSnapshotLifecycleStatus {
            snapshot_id: Some(snapshot.meta.snapshot_id.clone()),
            sending: true,
            installing: false,
            total_chunks: chunks.len() as u64,
            sent_chunks: 0,
            received_chunks: 0,
            retry_count: 0,
            throttled_ticks: 0,
            rolled_back: self.status.rolled_back,
            completed: false,
            installed_index: 0,
        };
        self.sender = Some(RaftSnapshotSendState {
            group_id: snapshot.group_id,
            term,
            leader_id,
            chunks,
            next_chunk: 0,
            accepted_chunks: 0,
            retry_count: 0,
        });
        Ok(())
    }

    pub fn poll_send_requests(&mut self) -> Result<Vec<InstallSnapshotRequest>, RaftError> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        let mut requests = Vec::new();
        let mut bytes = 0u64;
        while sender.next_chunk < sender.chunks.len()
            && requests.len() < self.config.max_chunks_per_tick as usize
        {
            let chunk = sender.chunks[sender.next_chunk].clone();
            let next_bytes = bytes + chunk.data.len() as u64;
            if !requests.is_empty() && next_bytes > self.config.max_bytes_per_tick {
                self.status.throttled_ticks += 1;
                break;
            }
            bytes = next_bytes;
            sender.next_chunk += 1;
            self.status.sent_chunks += 1;
            requests.push(InstallSnapshotRequest {
                group_id: sender.group_id,
                term: sender.term,
                leader_id: sender.leader_id,
                chunk,
            });
        }
        if sender.next_chunk < sender.chunks.len() {
            self.status.throttled_ticks += 1;
        }
        Ok(requests)
    }

    pub fn record_send_response(
        &mut self,
        response: &InstallSnapshotResponse,
    ) -> Result<(), RaftError> {
        if self.sender.is_none() && response.accepted {
            return Ok(());
        }
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        if response.accepted {
            sender.accepted_chunks += 1;
            if sender.accepted_chunks >= sender.chunks.len() as u64 {
                self.status.sending = false;
                self.status.completed = true;
                self.sender = None;
            }
            return Ok(());
        }
        sender.retry_count += 1;
        self.status.retry_count += 1;
        if sender.retry_count > self.config.max_retry_attempts {
            self.sender = None;
            self.status.sending = false;
            return Err(RaftError::Transport(
                "snapshot send retry budget exhausted".to_string(),
            ));
        }
        sender.next_chunk = sender
            .chunks
            .iter()
            .position(|chunk| chunk.offset >= response.next_offset)
            .unwrap_or(sender.chunks.len());
        Ok(())
    }

    pub fn record_send_timeout(&mut self) -> Result<(), RaftError> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        sender.retry_count += 1;
        self.status.retry_count += 1;
        if sender.retry_count > self.config.max_retry_attempts {
            self.sender = None;
            self.status.sending = false;
            return Err(RaftError::Transport(
                "snapshot send timeout retry budget exhausted".to_string(),
            ));
        }
        Ok(())
    }

    pub fn install_request(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<Option<RaftSnapshot>, RaftError> {
        if self.installer.is_none() {
            self.status.snapshot_id = Some(request.chunk.meta.snapshot_id.clone());
            self.status.installing = true;
            self.status.sending = false;
            self.status.completed = false;
            self.status.received_chunks = 0;
            self.status.total_chunks = 0;
            self.installer = Some(RaftSnapshotInstallState::new(request.chunk.meta.clone()));
        }
        let installer = self.installer.as_mut().expect("installer exists");
        installer.install_chunk(request.chunk)?;
        self.status.received_chunks += 1;
        self.status.total_chunks = self.status.total_chunks.max(self.status.received_chunks);
        if installer.complete {
            let installer = self.installer.take().expect("installer complete");
            let snapshot = installer.finish(request.group_id)?;
            self.status.installing = false;
            self.status.completed = true;
            self.status.installed_index = snapshot.meta.last_log_id.index;
            return Ok(Some(snapshot));
        }
        Ok(None)
    }

    pub fn rollback_install(&mut self) {
        self.installer = None;
        self.status.installing = false;
        self.status.rolled_back += 1;
    }

    pub fn status(&self) -> RaftSnapshotLifecycleStatus {
        self.status.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistentRaftSnapshotStoreOptions {
    pub dir: PathBuf,
    pub chunk_size: u64,
}

impl PersistentRaftSnapshotStoreOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            chunk_size: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistentRaftSnapshotStore {
    options: PersistentRaftSnapshotStoreOptions,
}

impl PersistentRaftSnapshotStore {
    pub fn open(options: PersistentRaftSnapshotStoreOptions) -> Result<Self, RaftError> {
        if options.chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot store chunk_size must be greater than zero".to_string(),
            ));
        }
        fs::create_dir_all(&options.dir).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create snapshot directory {}: {err}",
                options.dir.display()
            ))
        })?;
        Ok(Self { options })
    }

    pub fn save_checkpoint(&self, snapshot: &RaftSnapshot) -> Result<PathBuf, RaftError> {
        let path = self.snapshot_path(&snapshot.meta.snapshot_id);
        let encoded = serde_json::to_vec(snapshot)
            .map_err(|err| RaftError::Storage(format!("failed to encode snapshot: {err}")))?;
        let mut file = File::create(&path).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create snapshot {}: {err}",
                path.display()
            ))
        })?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|err| {
                RaftError::Storage(format!(
                    "failed to persist snapshot {}: {err}",
                    path.display()
                ))
            })?;
        Ok(path)
    }

    pub fn load_checkpoint(&self, snapshot_id: &str) -> Result<RaftSnapshot, RaftError> {
        let path = self.snapshot_path(snapshot_id);
        let bytes = fs::read(&path).map_err(|err| {
            RaftError::Storage(format!("failed to read snapshot {}: {err}", path.display()))
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|err| RaftError::Storage(format!("failed to decode snapshot: {err}")))
    }

    pub fn checkpoint_chunks(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<RustRaftSnapshotChunk>, RaftError> {
        let snapshot = self.load_checkpoint(snapshot_id)?;
        RaftSnapshotLifecycle::checkpoint(&snapshot, self.options.chunk_size)
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.options.dir.join(format!("{snapshot_id}.snapshot"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotRequest {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub chunk: RustRaftSnapshotChunk,
}

pub type InstallSnapshotRequest = RustRaftInstallSnapshotRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotResponse {
    pub term: RustRaftTerm,
    pub accepted: bool,
    pub next_offset: u64,
    pub reason: String,
}

pub type InstallSnapshotResponse = RustRaftInstallSnapshotResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexRequest {
    pub group_id: RustRaftGroupId,
    pub requester_id: RustRaftNodeId,
    pub min_commit_index: RustRaftLogIndex,
    pub allow_lease_read: bool,
}

pub type ReadIndexRequest = RustRaftReadIndexRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexResponse {
    pub safe: bool,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub reason: String,
}

pub type ReadIndexResponse = RustRaftReadIndexResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyDecision {
    pub safe: bool,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftReadSafetyOperation {
    ReadIndex,
    LeaseRead,
    BoundedStaleRead,
    Write,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyRuntimeInput {
    pub operation: RustRaftReadSafetyOperation,
    pub node_id: u64,
    pub leader_id: u64,
    pub node_alive: bool,
    pub role_can_serve_data: bool,
    pub leader_lease_valid: bool,
    pub has_majority: bool,
    pub node_commit_index: u64,
    pub leader_commit_index: u64,
    pub max_stale_index_lag: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadSafetyRuntimeDecision {
    pub allowed: bool,
    pub read_index: u64,
    pub reason: String,
    pub stale_leader_lease_rejected: bool,
    pub lagging_follower_read_rejected: bool,
    pub stale_follower_write_rejected: bool,
    pub minority_partition_read_rejected: bool,
    pub minority_partition_write_rejected: bool,
    pub healed_follower_catchup_observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftLearnerPromotionDecision {
    pub promotable: bool,
    pub learner_id: u64,
    pub learner_match_index: u64,
    pub required_match_index: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftAppendSafetyDecision {
    pub accepted: bool,
    pub rejected_compacted_entry: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RustRaftError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("node {0} not found")]
    NodeNotFound(RustRaftNodeId),
    #[error("no leader is available")]
    NoLeader,
    #[error("node {0} is not the leader")]
    NotLeader(RustRaftNodeId),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl From<RaftConfigError> for RustRaftError {
    fn from(error: RaftConfigError) -> Self {
        Self::Config(error.to_string())
    }
}

pub type RaftError = RustRaftError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RaftNode {
    id: RustRaftNodeId,
    replica_role: RustRaftReplicaRole,
    raft_role: RustRaftRole,
    hard_state: RustRaftHardState,
    log: Vec<RustRaftLogEntry>,
    installed_snapshot: Option<RustRaftSnapshotMeta>,
    commit_index: RustRaftLogIndex,
    applied_index: RustRaftLogIndex,
    healthy: bool,
}

impl RaftNode {
    fn new(id: RustRaftNodeId, replica_role: RustRaftReplicaRole) -> Self {
        Self {
            id,
            replica_role,
            raft_role: if replica_role == RustRaftReplicaRole::Learner {
                RustRaftRole::Learner
            } else {
                RustRaftRole::Follower
            },
            hard_state: RustRaftHardState {
                current_term: 0,
                voted_for: None,
                committed: None,
            },
            log: Vec::new(),
            installed_snapshot: None,
            commit_index: 0,
            applied_index: 0,
            healthy: true,
        }
    }

    fn match_index(&self) -> RustRaftLogIndex {
        let log_index = self
            .log
            .last()
            .map(|entry| entry.log_id.index)
            .unwrap_or_default();
        let snapshot_index = self
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_log_id.index)
            .unwrap_or_default();
        log_index.max(snapshot_index)
    }

    fn append_entry(&mut self, entry: RustRaftLogEntry) {
        if let Some(position) = self
            .log
            .iter()
            .position(|existing| existing.log_id.index == entry.log_id.index)
        {
            self.log.truncate(position);
        }
        self.log.push(entry);
    }

    fn advance_commit(&mut self, commit_index: RustRaftLogIndex) {
        self.commit_index = self.commit_index.max(commit_index.min(self.match_index()));
        if self.replica_role.can_serve_data() {
            self.applied_index = self.applied_index.max(self.commit_index);
        }
        self.hard_state.committed = (self.commit_index > 0).then_some(RustRaftLogId {
            term: self.hard_state.current_term,
            index: self.commit_index,
        });
    }

    fn install_snapshot(&mut self, snapshot: RaftSnapshot) {
        let snapshot_index = snapshot.meta.last_log_id.index;
        self.installed_snapshot = Some(snapshot.meta);
        self.log.retain(|entry| entry.log_id.index > snapshot_index);
        self.commit_index = self.commit_index.max(snapshot_index);
        if self.replica_role.can_serve_data() {
            self.applied_index = self.applied_index.max(snapshot_index);
        }
        self.hard_state.committed = Some(RustRaftLogId {
            term: self.hard_state.current_term,
            index: self.commit_index,
        });
    }

    fn compact_log_through(&mut self, log_index: RustRaftLogIndex) -> u64 {
        let before = self.log.len();
        self.log.retain(|entry| entry.log_id.index > log_index);
        before.saturating_sub(self.log.len()) as u64
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftCluster {
    pub group_id: RustRaftGroupId,
    pub config: RaftConfig,
    nodes: BTreeMap<RustRaftNodeId, RaftNode>,
    peer_pipelines: BTreeMap<RustRaftNodeId, RaftReplicationPipeline>,
    leader_id: Option<RustRaftNodeId>,
    current_term: RustRaftTerm,
    commit_index: RustRaftLogIndex,
    applied_index: RustRaftLogIndex,
    last_log_index: RustRaftLogIndex,
    running: bool,
    leader_lease_valid: bool,
}

impl RaftCluster {
    pub fn new(
        group_id: RustRaftGroupId,
        config: RaftConfig,
        peers: Vec<RustRaftPeer>,
    ) -> Result<Self, RaftError> {
        config.validate()?;
        if peers.is_empty() {
            return Err(RaftError::InvalidRequest(
                "raft cluster requires at least one peer".to_string(),
            ));
        }

        let mut nodes = BTreeMap::new();
        for peer in peers {
            if nodes
                .insert(peer.node_id, RaftNode::new(peer.node_id, peer.role))
                .is_some()
            {
                return Err(RaftError::InvalidRequest(format!(
                    "duplicate raft node id {}",
                    peer.node_id
                )));
            }
        }
        if !nodes.values().any(|node| node.replica_role.can_be_leader()) {
            return Err(RaftError::InvalidRequest(
                "raft cluster requires at least one voter".to_string(),
            ));
        }

        let peer_pipelines = nodes
            .keys()
            .copied()
            .map(|node_id| {
                (
                    node_id,
                    RaftReplicationPipeline::new(node_id, 1, RustRaftPipelineLimits::default()),
                )
            })
            .collect();

        Ok(Self {
            group_id,
            config,
            nodes,
            peer_pipelines,
            leader_id: None,
            current_term: 0,
            commit_index: 0,
            applied_index: 0,
            last_log_index: 0,
            running: false,
            leader_lease_valid: false,
        })
    }

    pub fn start(&mut self) -> Result<(), RaftError> {
        self.running = true;
        if self.leader_id.is_none() {
            let leader = self
                .nodes
                .values()
                .find(|node| node.replica_role.can_be_leader() && node.healthy)
                .map(|node| node.id)
                .ok_or_else(|| RaftError::InvalidRequest("no healthy voter".to_string()))?;
            self.campaign(leader, true)?;
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.running = false;
        self.leader_lease_valid = false;
        Ok(())
    }

    pub fn leader_id(&self) -> Option<RustRaftNodeId> {
        self.leader_id
    }

    pub fn set_node_healthy(
        &mut self,
        node_id: RustRaftNodeId,
        healthy: bool,
    ) -> Result<(), RaftError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        node.healthy = healthy;
        if self.leader_id == Some(node_id) && !healthy {
            self.leader_lease_valid = false;
        }
        Ok(())
    }

    pub fn set_leader_lease_valid(&mut self, valid: bool) {
        self.leader_lease_valid = valid;
    }

    pub fn campaign(
        &mut self,
        candidate_id: RustRaftNodeId,
        forced: bool,
    ) -> Result<(), RaftError> {
        if !self.running && !forced {
            return Err(RaftError::InvalidRequest(
                "cannot campaign before cluster start".to_string(),
            ));
        }
        let candidate = self
            .nodes
            .get(&candidate_id)
            .ok_or(RaftError::NodeNotFound(candidate_id))?;
        if !candidate.replica_role.can_be_leader() {
            return Err(RaftError::InvalidRequest(format!(
                "node {} cannot become leader",
                candidate_id
            )));
        }
        if !candidate.healthy {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not healthy",
                candidate_id
            )));
        }

        self.current_term += 1;
        self.leader_id = Some(candidate_id);
        self.leader_lease_valid = true;
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = self.current_term;
            node.hard_state.voted_for = Some(candidate_id);
            node.raft_role = if node.id == candidate_id {
                RustRaftRole::Leader
            } else if node.replica_role == RustRaftReplicaRole::Learner {
                RustRaftRole::Learner
            } else {
                RustRaftRole::Follower
            };
        }
        self.refresh_replication_pipelines();
        Ok(())
    }

    pub fn propose(&mut self, payload: RustRaftPayload) -> Result<RustRaftLogId, RaftError> {
        let leader_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        if !self.running {
            return Err(RaftError::InvalidRequest(
                "cannot propose while cluster is stopped".to_string(),
            ));
        }
        if payload.len() as u64 > self.config.max_payload_bytes {
            return Err(RaftError::InvalidRequest(
                "payload exceeds max_payload_bytes".to_string(),
            ));
        }
        let leader = self
            .nodes
            .get(&leader_id)
            .ok_or(RaftError::NodeNotFound(leader_id))?;
        if !leader.healthy {
            return Err(RaftError::NotLeader(leader_id));
        }

        let log_id = RustRaftLogId {
            term: self.current_term,
            index: self.last_log_index + 1,
        };
        let entry = RustRaftLogEntry {
            log_id: log_id.clone(),
            payload,
        };
        self.last_log_index = log_id.index;

        let node_ids: Vec<_> = self.nodes.keys().copied().collect();
        for node_id in node_ids {
            let Some(node) = self.nodes.get_mut(&node_id) else {
                continue;
            };
            if node_id == leader_id {
                node.append_entry(entry.clone());
                continue;
            }
            if !node.replica_role.can_serve_data() {
                continue;
            }
            let response = if let Some(pipeline) = self.peer_pipelines.get_mut(&node_id) {
                pipeline.queue_append(entry.clone())?;
                let _ = pipeline.flush_append_window();
                if node.healthy {
                    node.append_entry(entry.clone());
                    let match_index = node.match_index();
                    Some(RustRaftAppendEntriesResponse {
                        term: node.hard_state.current_term,
                        success: true,
                        match_index,
                        rejection_hint: None,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(response) = response {
                if let Some(pipeline) = self.peer_pipelines.get_mut(&node_id) {
                    let _ = pipeline.handle_append_response(&response);
                }
            }
        }
        self.refresh_commit_index();
        Ok(log_id)
    }

    pub fn append_entries_to(
        &mut self,
        target: RustRaftNodeId,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RaftError> {
        let node = self
            .nodes
            .get_mut(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "append entries group id mismatch".to_string(),
            ));
        }
        if request.term < node.hard_state.current_term {
            return Ok(RustRaftAppendEntriesResponse {
                term: node.hard_state.current_term,
                success: false,
                match_index: node.match_index(),
                rejection_hint: Some(node.match_index()),
            });
        }
        if let Some(prev) = &request.prev_log_id {
            if prev.index > 0 && node.match_index() < prev.index {
                return Ok(RustRaftAppendEntriesResponse {
                    term: node.hard_state.current_term,
                    success: false,
                    match_index: node.match_index(),
                    rejection_hint: Some(node.match_index()),
                });
            }
        }

        node.hard_state.current_term = request.term;
        node.raft_role = if node.replica_role == RustRaftReplicaRole::Learner {
            RustRaftRole::Learner
        } else {
            RustRaftRole::Follower
        };
        for entry in request.entries {
            if node.replica_role.can_serve_data() {
                node.append_entry(entry);
            }
        }
        node.advance_commit(request.leader_commit);
        let term = node.hard_state.current_term;
        let match_index = node.match_index();
        self.refresh_cluster_indexes();
        Ok(RustRaftAppendEntriesResponse {
            term,
            success: true,
            match_index,
            rejection_hint: None,
        })
    }

    pub fn read_index(
        &self,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "read index group id mismatch".to_string(),
            ));
        }
        let node = self
            .nodes
            .get(&request.requester_id)
            .ok_or(RaftError::NodeNotFound(request.requester_id))?;
        if !node.healthy {
            return Ok(RustRaftReadIndexResponse {
                safe: false,
                read_index: node.commit_index,
                lease_read: false,
                reason: "node_unhealthy".to_string(),
            });
        }
        if request.min_commit_index > node.applied_index {
            return Ok(RustRaftReadIndexResponse {
                safe: false,
                read_index: node.commit_index,
                lease_read: false,
                reason: "applied_index_behind_min_commit".to_string(),
            });
        }
        let lease_read = request.allow_lease_read
            && self.config.enable_lease_read
            && self.leader_id == Some(request.requester_id)
            && self.leader_lease_valid;
        Ok(RustRaftReadIndexResponse {
            safe: true,
            read_index: node.commit_index,
            lease_read,
            reason: if lease_read {
                "lease_read".to_string()
            } else {
                "read_index".to_string()
            },
        })
    }

    pub fn lease_read_eligible(
        &self,
        node_id: RustRaftNodeId,
        min_commit_index: RustRaftLogIndex,
    ) -> Result<bool, RaftError> {
        let response = self.read_index(RustRaftReadIndexRequest {
            group_id: self.group_id,
            requester_id: node_id,
            min_commit_index,
            allow_lease_read: true,
        })?;
        Ok(response.safe && response.lease_read)
    }

    pub fn transfer_leader(&mut self, target: RustRaftNodeId) -> Result<(), RaftError> {
        let target_node = self
            .nodes
            .get(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        if !target_node.replica_role.can_be_leader() {
            return Err(RaftError::InvalidRequest(format!(
                "node {} cannot become leader",
                target
            )));
        }
        if !target_node.healthy {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is not healthy",
                target
            )));
        }
        if target_node.match_index() < self.commit_index {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is behind committed index {}",
                target, self.commit_index
            )));
        }
        self.campaign(target, true)
    }

    pub fn membership(&self) -> RaftMembership {
        RaftMembership {
            group_id: self.group_id,
            voters: self
                .nodes
                .values()
                .filter(|node| node.replica_role == RustRaftReplicaRole::Voter)
                .map(|node| node.id)
                .collect(),
            learners: self
                .nodes
                .values()
                .filter(|node| node.replica_role == RustRaftReplicaRole::Learner)
                .map(|node| node.id)
                .collect(),
            witnesses: self
                .nodes
                .values()
                .filter(|node| node.replica_role == RustRaftReplicaRole::Witness)
                .map(|node| node.id)
                .collect(),
            epoch: self.current_term,
        }
    }

    pub fn add_peer(&mut self, peer: RustRaftPeer) -> Result<(), RaftError> {
        if self.nodes.contains_key(&peer.node_id) {
            return Err(RaftError::InvalidRequest(format!(
                "duplicate raft node id {}",
                peer.node_id
            )));
        }
        self.nodes
            .insert(peer.node_id, RaftNode::new(peer.node_id, peer.role));
        self.peer_pipelines.insert(
            peer.node_id,
            RaftReplicationPipeline::new(
                peer.node_id,
                self.last_log_index + 1,
                RustRaftPipelineLimits::default(),
            ),
        );
        self.refresh_replication_pipelines();
        Ok(())
    }

    pub fn add_learner(&mut self, mut peer: RustRaftPeer) -> Result<(), RaftError> {
        peer.role = RustRaftReplicaRole::Learner;
        self.add_peer(peer)
    }

    pub fn promote_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        let commit_index = self.commit_index;
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        if node.match_index() < commit_index {
            return Err(RaftError::InvalidRequest(format!(
                "node {} is behind committed index {}",
                node_id, commit_index
            )));
        }
        node.replica_role = RustRaftReplicaRole::Voter;
        node.raft_role = RustRaftRole::Follower;
        Ok(())
    }

    pub fn add_witness(&mut self, mut peer: RustRaftPeer) -> Result<(), RaftError> {
        peer.role = RustRaftReplicaRole::Witness;
        self.add_peer(peer)
    }

    pub fn remove_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        self.nodes
            .remove(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        self.peer_pipelines.remove(&node_id);
        if self.leader_id == Some(node_id) {
            self.leader_id = None;
            self.leader_lease_valid = false;
        }
        self.refresh_replication_pipelines();
        Ok(())
    }

    pub fn catchup_report(
        &self,
        learner_id: RustRaftNodeId,
    ) -> Result<RaftLearnerCatchUpReport, RaftError> {
        let node = self
            .nodes
            .get(&learner_id)
            .ok_or(RaftError::NodeNotFound(learner_id))?;
        Ok(self
            .membership()
            .catchup_report(learner_id, node.match_index(), self.commit_index))
    }

    pub fn install_snapshot_to(
        &mut self,
        target: RustRaftNodeId,
        snapshot: RaftSnapshot,
        fence: RustRaftApplySnapshotFence,
    ) -> Result<(), RaftError> {
        if snapshot.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "snapshot group id mismatch".to_string(),
            ));
        }
        rustraft_validate_snapshot_install(&snapshot, &fence)?;
        let node = self
            .nodes
            .get_mut(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        let snapshot_index = snapshot.meta.last_log_id.index;
        node.install_snapshot(snapshot);
        if let Some(pipeline) = self.peer_pipelines.get_mut(&target) {
            pipeline
                .begin_snapshot_install(
                    format!("installed-{target}-{snapshot_index}"),
                    snapshot_index,
                    1,
                )
                .ok();
            pipeline.receive_snapshot_chunk(0, true).ok();
        }
        self.refresh_cluster_indexes();
        Ok(())
    }

    pub fn install_snapshot_with_tail_to(
        &mut self,
        target: RustRaftNodeId,
        snapshot: RaftSnapshot,
        fence: RustRaftApplySnapshotFence,
        tail_entries: Vec<RustRaftLogEntry>,
    ) -> Result<(), RaftError> {
        let snapshot_index = snapshot.meta.last_log_id.index;
        if let Some(first_tail) = tail_entries.first() {
            if first_tail.log_id.index <= snapshot_index {
                return Err(RaftError::InvalidRequest(
                    "tail catch-up entry overlaps installed snapshot".to_string(),
                ));
            }
        }
        self.install_snapshot_to(target, snapshot, fence)?;
        let node = self
            .nodes
            .get_mut(&target)
            .ok_or(RaftError::NodeNotFound(target))?;
        for entry in tail_entries {
            node.append_entry(entry);
        }
        node.advance_commit(self.commit_index.max(node.match_index()));
        if let Some(pipeline) = self.peer_pipelines.get_mut(&target) {
            let response = RustRaftAppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: node.match_index(),
                rejection_hint: None,
            };
            let _ = pipeline.handle_append_response(&response);
            pipeline.mark_snapshot_rejoin_after_compacted_log();
        }
        self.refresh_cluster_indexes();
        Ok(())
    }

    pub fn compact_logs_through(&mut self, log_index: RustRaftLogIndex) -> u64 {
        let removed = self
            .nodes
            .values_mut()
            .map(|node| node.compact_log_through(log_index))
            .sum();
        self.refresh_cluster_indexes();
        removed
    }

    pub fn checkpoint_snapshot(
        &self,
        node_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<RaftSnapshot, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        let snapshot_index = node.commit_index.max(node.match_index());
        Ok(RaftSnapshot {
            group_id: self.group_id,
            meta: RustRaftSnapshotMeta {
                snapshot_id: snapshot_id.into(),
                last_log_id: RustRaftLogId {
                    term: node.hard_state.current_term,
                    index: snapshot_index,
                },
                membership: self.node_ids(),
            },
            payload: serde_json::to_vec(&node.log).map_err(|err| {
                RaftError::Storage(format!("failed to encode checkpoint snapshot: {err}"))
            })?,
        })
    }

    pub fn install_snapshot_chunk_to(
        &mut self,
        target: RustRaftNodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "snapshot install group id mismatch".to_string(),
            ));
        }
        let next_offset = request.chunk.offset + request.chunk.data.len() as u64;
        if !request.chunk.done {
            return Ok(InstallSnapshotResponse {
                term: self.current_term.max(request.term),
                accepted: true,
                next_offset,
                reason: "snapshot_chunk_accepted".to_string(),
            });
        }

        let mut install = RaftSnapshotInstallState::new(request.chunk.meta.clone());
        install.install_chunk(request.chunk)?;
        let snapshot = install.finish(self.group_id)?;
        let snapshot_index = snapshot.meta.last_log_id.index;
        self.install_snapshot_to(
            target,
            snapshot,
            RustRaftApplySnapshotFence {
                applied_index: snapshot_index,
                commit_index: snapshot_index,
                installed_snapshot_index: snapshot_index,
                first_retained_log_index: snapshot_index + 1,
            },
        )?;
        Ok(InstallSnapshotResponse {
            term: self.current_term.max(request.term),
            accepted: true,
            next_offset,
            reason: "snapshot_installed".to_string(),
        })
    }

    pub fn install_snapshot_lifecycle_request_to(
        &mut self,
        target: RustRaftNodeId,
        lifecycle: &mut RaftSnapshotLifecycle,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(
                "snapshot lifecycle group id mismatch".to_string(),
            ));
        }
        let next_offset = request.chunk.offset + request.chunk.data.len() as u64;
        match lifecycle.install_request(request)? {
            Some(snapshot) => {
                let snapshot_index = snapshot.meta.last_log_id.index;
                self.install_snapshot_to(
                    target,
                    snapshot,
                    RustRaftApplySnapshotFence {
                        applied_index: snapshot_index,
                        commit_index: snapshot_index,
                        installed_snapshot_index: snapshot_index,
                        first_retained_log_index: snapshot_index + 1,
                    },
                )?;
                Ok(InstallSnapshotResponse {
                    term: self.current_term,
                    accepted: true,
                    next_offset,
                    reason: "snapshot_installed".to_string(),
                })
            }
            None => Ok(InstallSnapshotResponse {
                term: self.current_term,
                accepted: true,
                next_offset,
                reason: "snapshot_chunk_accepted".to_string(),
            }),
        }
    }

    pub fn status(&self, node_id: RustRaftNodeId) -> Result<RustRaftStatusSnapshot, RaftError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(RaftError::NodeNotFound(node_id))?;
        Ok(RustRaftStatusSnapshot {
            group_id: self.group_id,
            node_id,
            role: node.raft_role,
            term: node.hard_state.current_term,
            leader_id: self.leader_id,
            commit_index: node.commit_index,
            applied_index: node.applied_index,
            last_log_index: node.match_index(),
            last_snapshot_index: node
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_log_id.index)
                .unwrap_or_default(),
            peers: self
                .nodes
                .values()
                .filter(|peer| peer.id != node_id)
                .map(|peer| RustRaftPeerStatus {
                    node_id: peer.id,
                    matched: peer.match_index(),
                    next_index: peer.match_index() + 1,
                    learner: peer.replica_role == RustRaftReplicaRole::Learner,
                    healthy: peer.healthy,
                    lag: node.match_index().saturating_sub(peer.match_index()),
                })
                .collect(),
        })
    }

    pub fn cluster_status_report(&self) -> Result<RaftClusterStatusReport, RaftError> {
        let nodes = self
            .node_ids()
            .into_iter()
            .map(|node_id| self.status(node_id))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rustraft_cluster_status_report(
            self.group_id,
            self.leader_id,
            nodes,
        ))
    }

    pub fn peer_pipeline_status(
        &self,
        peer_id: RustRaftNodeId,
    ) -> Result<RaftPeerPipelineState, RaftError> {
        self.peer_pipelines
            .get(&peer_id)
            .map(RaftReplicationPipeline::status)
            .ok_or(RaftError::NodeNotFound(peer_id))
    }

    pub fn peer_pipeline_statuses(&self) -> Vec<RaftPeerPipelineState> {
        self.peer_pipelines
            .iter()
            .filter(|(peer_id, _)| Some(**peer_id) != self.leader_id)
            .map(|(_, pipeline)| pipeline.status())
            .collect()
    }

    pub fn receive_out_of_order_append_for(
        &mut self,
        peer_id: RustRaftNodeId,
        entry: RustRaftLogEntry,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .receive_out_of_order(entry)
    }

    pub fn expire_peer_reorder_queue(&mut self, peer_id: RustRaftNodeId) -> Result<u64, RaftError> {
        Ok(self
            .peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .expire_reorder_queue())
    }

    pub fn begin_snapshot_send_to(
        &mut self,
        peer_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .begin_snapshot_send(snapshot_id, snapshot_index, total_chunks)
    }

    pub fn record_snapshot_chunk_sent_to(
        &mut self,
        peer_id: RustRaftNodeId,
        bytes: u64,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .record_snapshot_chunk_sent(bytes)
    }

    pub fn acknowledge_snapshot_chunk_to(
        &mut self,
        peer_id: RustRaftNodeId,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .acknowledge_snapshot_chunk()
    }

    pub fn begin_snapshot_install_from(
        &mut self,
        peer_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .begin_snapshot_install(snapshot_id, snapshot_index, total_chunks)
    }

    pub fn receive_snapshot_chunk_from(
        &mut self,
        peer_id: RustRaftNodeId,
        bytes: u64,
        done: bool,
    ) -> Result<(), RaftError> {
        self.peer_pipelines
            .get_mut(&peer_id)
            .ok_or(RaftError::NodeNotFound(peer_id))?
            .receive_snapshot_chunk(bytes, done)
    }

    pub fn node_ids(&self) -> Vec<RustRaftNodeId> {
        self.nodes.keys().copied().collect()
    }

    fn refresh_commit_index(&mut self) {
        let mut candidate_indexes: Vec<_> = self
            .nodes
            .values()
            .filter(|node| node.healthy && node.replica_role.participates_in_quorum())
            .map(RaftNode::match_index)
            .collect();
        if candidate_indexes.is_empty() {
            return;
        }
        candidate_indexes.sort_unstable();
        let quorum_index = candidate_indexes.len().saturating_sub(self.quorum_size());
        let commit_index = candidate_indexes[quorum_index];
        self.commit_index = self.commit_index.max(commit_index);
        for node in self.nodes.values_mut() {
            if node.healthy {
                node.advance_commit(self.commit_index);
            }
        }
        self.refresh_cluster_indexes();
    }

    fn refresh_cluster_indexes(&mut self) {
        self.commit_index = self
            .nodes
            .values()
            .map(|node| node.commit_index)
            .max()
            .unwrap_or_default();
        self.applied_index = self
            .nodes
            .values()
            .filter(|node| node.replica_role.can_serve_data())
            .map(|node| node.applied_index)
            .min()
            .unwrap_or_default();
        self.last_log_index = self
            .nodes
            .values()
            .map(RaftNode::match_index)
            .max()
            .unwrap_or_default();
    }

    fn quorum_size(&self) -> usize {
        let voters = self
            .nodes
            .values()
            .filter(|node| node.replica_role.participates_in_quorum())
            .count();
        voters / 2 + 1
    }

    fn refresh_replication_pipelines(&mut self) {
        for (node_id, node) in &self.nodes {
            self.peer_pipelines.entry(*node_id).or_insert_with(|| {
                RaftReplicationPipeline::new(
                    *node_id,
                    node.match_index() + 1,
                    RustRaftPipelineLimits::default(),
                )
            });
        }
        if let Some(leader_id) = self.leader_id {
            if let Some(pipeline) = self.peer_pipelines.get_mut(&leader_id) {
                let response = RustRaftAppendEntriesResponse {
                    term: self.current_term,
                    success: true,
                    match_index: self.last_log_index,
                    rejection_hint: None,
                };
                let _ = pipeline.handle_append_response(&response);
            }
        }
    }
}

impl RustRaftConsensus for RaftCluster {
    fn start(&mut self) -> Result<(), RustRaftError> {
        RaftCluster::start(self)
    }

    fn stop(&mut self) -> Result<(), RustRaftError> {
        RaftCluster::stop(self)
    }

    fn status(&self) -> Result<RustRaftStatusSnapshot, RustRaftError> {
        let node_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        RaftCluster::status(self, node_id)
    }

    fn propose(
        &mut self,
        payload: RustRaftPayload,
        options: RustRaftProposeOptions,
    ) -> Result<RustRaftLogId, RustRaftError> {
        if let Some(expected_term) = options.expected_term {
            if expected_term != self.current_term {
                return Err(RaftError::InvalidRequest(format!(
                    "expected term {} does not match current term {}",
                    expected_term, self.current_term
                )));
            }
        }
        RaftCluster::propose(self, payload)
    }

    fn read_index(
        &self,
        min_commit_index: RustRaftLogIndex,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError> {
        let requester_id = self.leader_id.ok_or(RaftError::NoLeader)?;
        RaftCluster::read_index(
            self,
            RustRaftReadIndexRequest {
                group_id: self.group_id,
                requester_id,
                min_commit_index,
                allow_lease_read: false,
            },
        )
    }

    fn add_peer(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError> {
        RaftCluster::add_peer(self, peer)
    }

    fn add_learner(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError> {
        RaftCluster::add_learner(self, peer)
    }

    fn promote_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError> {
        RaftCluster::promote_peer(self, node_id)
    }

    fn add_witness(&mut self, peer: RustRaftPeer) -> Result<(), RustRaftError> {
        RaftCluster::add_witness(self, peer)
    }

    fn remove_peer(&mut self, node_id: RustRaftNodeId) -> Result<(), RustRaftError> {
        RaftCluster::remove_peer(self, node_id)
    }

    fn transfer_leader(&mut self, target: RustRaftNodeId) -> Result<(), RustRaftError> {
        RaftCluster::transfer_leader(self, target)
    }

    fn campaign(&mut self, forced: bool) -> Result<(), RustRaftError> {
        let candidate_id = self.leader_id.unwrap_or_else(|| {
            self.nodes
                .values()
                .find(|node| node.replica_role.can_be_leader())
                .map(|node| node.id)
                .unwrap_or_default()
        });
        RaftCluster::campaign(self, candidate_id, forced)
    }

    fn trigger_snapshot(&mut self) -> Result<RustRaftSnapshotMeta, RustRaftError> {
        Ok(RustRaftSnapshotMeta {
            snapshot_id: format!("{}-{}", self.group_id, self.commit_index),
            last_log_id: RustRaftLogId {
                term: self.current_term,
                index: self.commit_index,
            },
            membership: self.node_ids(),
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftNodeRuntimeState {
    Created,
    Running,
    Stopped,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeRuntimeStatus {
    pub node_id: RustRaftNodeId,
    pub group_id: RustRaftGroupId,
    pub state: RaftNodeRuntimeState,
    pub restart_count: u64,
    pub worker_running: bool,
    pub cluster_status: Option<RaftClusterStatusReport>,
}

enum RaftNodeRuntimeOp {
    Start(mpsc::Sender<Result<(), RaftError>>),
    Stop(mpsc::Sender<Result<(), RaftError>>),
    Status(mpsc::Sender<Result<RaftNodeRuntimeStatus, RaftError>>),
    Propose(
        RustRaftPayload,
        mpsc::Sender<Result<RustRaftLogId, RaftError>>,
    ),
    ReadIndex(
        RustRaftLogIndex,
        mpsc::Sender<Result<ReadIndexResponse, RaftError>>,
    ),
    TransferLeader(RustRaftNodeId, mpsc::Sender<Result<(), RaftError>>),
    Campaign(bool, mpsc::Sender<Result<(), RaftError>>),
    Shutdown(mpsc::Sender<Result<(), RaftError>>),
}

#[derive(Debug)]
pub struct RaftNodeRuntime {
    node_id: RustRaftNodeId,
    group_id: RustRaftGroupId,
    command_tx: Option<mpsc::Sender<RaftNodeRuntimeOp>>,
    worker: Option<thread::JoinHandle<()>>,
    restart_count: u64,
    state: RaftNodeRuntimeState,
}

impl RaftNodeRuntime {
    pub fn create(options: RustRaftNodeOptions) -> Result<Self, RaftError> {
        let node_id = options.node_id;
        let group_id = options.group_id;
        let (command_tx, command_rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name(format!("rustraft-node-{group_id}-{node_id}"))
            .spawn(move || raft_node_runtime_loop(options, command_rx))
            .map_err(|err| RaftError::Transport(format!("failed to spawn raft node: {err}")))?;
        Ok(Self {
            node_id,
            group_id,
            command_tx: Some(command_tx),
            worker: Some(worker),
            restart_count: 0,
            state: RaftNodeRuntimeState::Created,
        })
    }

    pub fn start(&mut self) -> Result<(), RaftError> {
        self.send_unit(RaftNodeRuntimeOp::Start)?;
        self.state = RaftNodeRuntimeState::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.send_unit(RaftNodeRuntimeOp::Stop)?;
        self.state = RaftNodeRuntimeState::Stopped;
        Ok(())
    }

    pub fn restart(&mut self) -> Result<(), RaftError> {
        if self.state == RaftNodeRuntimeState::Shutdown {
            return Err(RaftError::InvalidRequest(
                "cannot restart a shutdown raft node runtime".to_string(),
            ));
        }
        if self.state == RaftNodeRuntimeState::Running {
            self.stop()?;
        }
        self.restart_count += 1;
        self.start()
    }

    pub fn shutdown(&mut self) -> Result<(), RaftError> {
        if self.state == RaftNodeRuntimeState::Shutdown {
            return Ok(());
        }
        let sender = self.command_tx.take().ok_or_else(|| {
            RaftError::InvalidRequest("raft node runtime channel is closed".to_string())
        })?;
        let (reply_tx, reply_rx) = mpsc::channel();
        sender
            .send(RaftNodeRuntimeOp::Shutdown(reply_tx))
            .map_err(|err| RaftError::Transport(format!("failed to shutdown raft node: {err}")))?;
        let result = recv_runtime_reply(reply_rx)?;
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| {
                RaftError::Transport("raft node worker panicked during shutdown".to_string())
            })?;
        }
        self.state = RaftNodeRuntimeState::Shutdown;
        result
    }

    pub fn propose(&self, payload: RustRaftPayload) -> Result<RustRaftLogId, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::Propose(payload, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send propose to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn read_index(
        &self,
        min_commit_index: RustRaftLogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::ReadIndex(min_commit_index, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send read-index to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn transfer_leader(&self, target: RustRaftNodeId) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::TransferLeader(target, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send leader transfer to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn campaign(&self, forced: bool) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::Campaign(forced, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send campaign to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn status(&self) -> Result<RaftNodeRuntimeStatus, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::Status(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send status to raft node: {err}"))
            })?;
        let mut status = recv_runtime_reply(reply_rx)??;
        status.restart_count = self.restart_count;
        status.state = self.state;
        Ok(status)
    }

    pub fn state(&self) -> RaftNodeRuntimeState {
        self.state
    }

    pub fn node_id(&self) -> RustRaftNodeId {
        self.node_id
    }

    pub fn group_id(&self) -> RustRaftGroupId {
        self.group_id
    }

    pub fn restart_count(&self) -> u64 {
        self.restart_count
    }

    fn send_unit(
        &self,
        command: fn(mpsc::Sender<Result<(), RaftError>>) -> RaftNodeRuntimeOp,
    ) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?.send(command(reply_tx)).map_err(|err| {
            RaftError::Transport(format!(
                "failed to send lifecycle command to raft node: {err}"
            ))
        })?;
        recv_runtime_reply(reply_rx)?
    }

    fn sender(&self) -> Result<&mpsc::Sender<RaftNodeRuntimeOp>, RaftError> {
        self.command_tx
            .as_ref()
            .ok_or_else(|| RaftError::InvalidRequest("raft node runtime is shut down".to_string()))
    }
}

impl Drop for RaftNodeRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn raft_node_runtime_loop(
    options: RustRaftNodeOptions,
    command_rx: mpsc::Receiver<RaftNodeRuntimeOp>,
) {
    let node_id = options.node_id;
    let group_id = options.group_id;
    let mut peers = options.peers.clone();
    if !peers.iter().any(|peer| peer.node_id == node_id) {
        peers.push(RustRaftPeer {
            node_id,
            raft_addr: options.raft_addr,
            snapshot_addr: options.snapshot_addr,
            role: options.role,
            auto_promote: false,
        });
    }
    let mut cluster = match RaftCluster::new(group_id, options.config, peers) {
        Ok(cluster) => cluster,
        Err(error) => {
            while let Ok(command) = command_rx.recv() {
                if respond_runtime_error(command, error.clone()) {
                    break;
                }
            }
            return;
        }
    };
    let mut state = RaftNodeRuntimeState::Created;
    while let Ok(command) = command_rx.recv() {
        match command {
            RaftNodeRuntimeOp::Start(reply) => {
                let result = cluster.start();
                if result.is_ok() {
                    state = RaftNodeRuntimeState::Running;
                }
                let _ = reply.send(result);
            }
            RaftNodeRuntimeOp::Stop(reply) => {
                let result = cluster.stop();
                if result.is_ok() {
                    state = RaftNodeRuntimeState::Stopped;
                }
                let _ = reply.send(result);
            }
            RaftNodeRuntimeOp::Status(reply) => {
                let status = RaftNodeRuntimeStatus {
                    node_id,
                    group_id,
                    state,
                    restart_count: 0,
                    worker_running: state != RaftNodeRuntimeState::Shutdown,
                    cluster_status: cluster.cluster_status_report().ok(),
                };
                let _ = reply.send(Ok(status));
            }
            RaftNodeRuntimeOp::Propose(payload, reply) => {
                let _ = reply.send(cluster.propose(payload));
            }
            RaftNodeRuntimeOp::ReadIndex(min_commit_index, reply) => {
                let request = ReadIndexRequest {
                    group_id,
                    requester_id: node_id,
                    min_commit_index,
                    allow_lease_read: true,
                };
                let _ = reply.send(cluster.read_index(request));
            }
            RaftNodeRuntimeOp::TransferLeader(target, reply) => {
                let _ = reply.send(cluster.transfer_leader(target));
            }
            RaftNodeRuntimeOp::Campaign(forced, reply) => {
                let _ = reply.send(cluster.campaign(node_id, forced));
            }
            RaftNodeRuntimeOp::Shutdown(reply) => {
                let result = cluster.stop();
                let _ = reply.send(result);
                break;
            }
        }
    }
}

fn respond_runtime_error(command: RaftNodeRuntimeOp, error: RaftError) -> bool {
    match command {
        RaftNodeRuntimeOp::Start(reply)
        | RaftNodeRuntimeOp::Stop(reply)
        | RaftNodeRuntimeOp::TransferLeader(_, reply)
        | RaftNodeRuntimeOp::Campaign(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::Shutdown(reply) => {
            let _ = reply.send(Err(error));
            true
        }
        RaftNodeRuntimeOp::Status(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::Propose(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::ReadIndex(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
    }
}

fn recv_runtime_reply<T>(reply_rx: mpsc::Receiver<T>) -> Result<T, RaftError> {
    reply_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|err| RaftError::Transport(format!("raft node runtime did not reply: {err}")))
}

pub trait RustRaftStorage {
    fn append_entries(&mut self, entries: &[RustRaftLogEntry]) -> Result<(), RustRaftError>;
    fn read_entries(&self, start: u64, end: u64) -> Result<Vec<RustRaftLogEntry>, RustRaftError>;
    fn hard_state(&self) -> Result<RustRaftHardState, RustRaftError>;
    fn install_snapshot(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RustRaftError>;
}

pub trait RustRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError>;
    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError>;
    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError>;
    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError>;
}

pub trait RaftTransport: RustRaftTransport {}

impl<T> RaftTransport for T where T: RustRaftTransport + ?Sized {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthenticatedRaftRpc<M, A = String> {
    pub auth: A,
    pub message: M,
}

pub trait RaftAuthPolicy<A = String> {
    fn token_for(&self, target: RustRaftNodeId) -> A;
    fn validate(&self, target: RustRaftNodeId, auth: &A) -> Result<(), RaftError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaticRaftAuthToken {
    pub token: String,
}

impl StaticRaftAuthToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl RaftAuthPolicy<String> for StaticRaftAuthToken {
    fn token_for(&self, _target: RustRaftNodeId) -> String {
        self.token.clone()
    }

    fn validate(&self, _target: RustRaftNodeId, auth: &String) -> Result<(), RaftError> {
        if auth == &self.token {
            Ok(())
        } else {
            Err(RaftError::Transport(
                "raft transport authentication failed".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedRaftTransport<T, A = String, P = StaticRaftAuthToken> {
    inner: T,
    policy: P,
    _auth: PhantomData<A>,
}

impl<T, A, P> AuthenticatedRaftTransport<T, A, P>
where
    P: RaftAuthPolicy<A>,
{
    pub fn new(inner: T, policy: P) -> Self {
        Self {
            inner,
            policy,
            _auth: PhantomData,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn wrap_request<M>(
        &self,
        target: RustRaftNodeId,
        message: M,
    ) -> AuthenticatedRaftRpc<M, A> {
        AuthenticatedRaftRpc {
            auth: self.policy.token_for(target),
            message,
        }
    }
}

impl<T, A, P> AuthenticatedRaftTransport<T, A, P>
where
    T: RustRaftTransport,
    P: RaftAuthPolicy<A>,
{
    pub fn append_entries_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<AppendEntriesRequest, A>,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.append_entries(target, request.message)
    }

    pub fn vote_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<VoteRequest, A>,
    ) -> Result<VoteResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.vote(target, request.message)
    }

    pub fn install_snapshot_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<InstallSnapshotRequest, A>,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.install_snapshot(target, request.message)
    }

    pub fn read_index_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<ReadIndexRequest, A>,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.read_index(target, request.message)
    }
}

impl<T, A, P> RustRaftTransport for AuthenticatedRaftTransport<T, A, P>
where
    T: RustRaftTransport,
    P: RaftAuthPolicy<A>,
{
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError> {
        self.inner.append_entries(target, request)
    }

    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError> {
        self.inner.vote(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError> {
        self.inner.install_snapshot(target, request)
    }

    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError> {
        self.inner.read_index(target, request)
    }
}

pub fn rustraft_wal_checksum(record: &RaftWalRecord) -> String {
    let mut hash = 14_695_981_039_346_656_037_u64;
    let mut mix = |value: u64| {
        for byte in value.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
    };
    mix(record.group_id);
    mix(record.node_id);
    mix(record.hard_state.current_term);
    mix(record.hard_state.voted_for.unwrap_or_default());
    if let Some(committed) = &record.hard_state.committed {
        mix(committed.term);
        mix(committed.index);
    }
    for entry in &record.entries {
        mix(entry.log_id.term);
        mix(entry.log_id.index);
        mix(entry.payload.len() as u64);
    }
    if let Some(snapshot) = &record.installed_snapshot {
        mix(snapshot.last_log_id.term);
        mix(snapshot.last_log_id.index);
    }
    mix(record.apply_snapshot_fence.applied_index);
    mix(record.apply_snapshot_fence.commit_index);
    mix(record.apply_snapshot_fence.installed_snapshot_index);
    mix(record.apply_snapshot_fence.first_retained_log_index);
    format!("{hash:016x}")
}

pub fn rustraft_wal_checksum_valid(record: &RaftWalRecord) -> bool {
    record.checksum == rustraft_wal_checksum(record)
}

pub fn rustraft_validate_apply_snapshot_fence(
    record: &RustRaftWalRecord,
) -> Result<(), RustRaftError> {
    let fence = &record.apply_snapshot_fence;
    let committed_index = record
        .hard_state
        .committed
        .as_ref()
        .map(|log_id| log_id.index)
        .unwrap_or_default();
    if fence.applied_index > committed_index {
        return Err(RustRaftError::Storage(
            "apply snapshot fence is ahead of committed index".to_string(),
        ));
    }
    if fence.commit_index != committed_index {
        return Err(RustRaftError::Storage(
            "apply snapshot fence commit index does not match hard state".to_string(),
        ));
    }
    if let Some(snapshot) = &record.installed_snapshot {
        if fence.installed_snapshot_index != snapshot.last_log_id.index {
            return Err(RustRaftError::Storage(
                "apply snapshot fence does not match installed snapshot".to_string(),
            ));
        }
        if fence.first_retained_log_index > 0
            && fence.first_retained_log_index <= snapshot.last_log_id.index
        {
            return Err(RustRaftError::Storage(
                "first retained log index overlaps installed snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn rustraft_validate_snapshot_floor_log_matching(
    snapshot: &RustRaftSnapshotMeta,
    first_retained_log_index: RustRaftLogIndex,
    prev_log_id: Option<&RustRaftLogId>,
) -> Result<(), RustRaftError> {
    if first_retained_log_index > 0 && first_retained_log_index <= snapshot.last_log_id.index {
        return Err(RaftError::Storage(
            "first retained log index overlaps snapshot floor".to_string(),
        ));
    }
    if let Some(prev_log_id) = prev_log_id {
        if prev_log_id.index < snapshot.last_log_id.index {
            return Err(RaftError::Storage(
                "previous log id is below snapshot floor".to_string(),
            ));
        }
        if prev_log_id.index == snapshot.last_log_id.index
            && prev_log_id.term != snapshot.last_log_id.term
        {
            return Err(RaftError::Storage(
                "snapshot floor term does not match previous log id".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn rustraft_validate_snapshot_install(
    snapshot: &RaftSnapshot,
    fence: &RustRaftApplySnapshotFence,
) -> Result<(), RustRaftError> {
    if fence.installed_snapshot_index != snapshot.meta.last_log_id.index {
        return Err(RaftError::Storage(
            "snapshot install fence does not match snapshot last log index".to_string(),
        ));
    }
    rustraft_validate_snapshot_floor_log_matching(
        &snapshot.meta,
        fence.first_retained_log_index,
        Some(&snapshot.meta.last_log_id),
    )
}

pub fn rustraft_recover_latest_wal_record(
    records: &[RustRaftWalRecord],
) -> Result<RustRaftWalRecord, RustRaftError> {
    let valid_records = records
        .iter()
        .take_while(|record| rustraft_wal_checksum_valid(record))
        .collect::<Vec<_>>();
    let Some(record) = valid_records
        .into_iter()
        .filter(|record| rustraft_validate_apply_snapshot_fence(record).is_ok())
        .max_by_key(|record| {
            record
                .hard_state
                .committed
                .as_ref()
                .map(|log_id| log_id.index)
                .unwrap_or_default()
        })
    else {
        return Err(RustRaftError::Storage(
            "no valid WAL record survived recovery".to_string(),
        ));
    };
    Ok(record.clone())
}

pub fn rustraft_parity_contract() -> RustRaftParityContract {
    RustRaftParityContract {
        library_name: "rustraft".to_string(),
        consensus_backend_boundary: "temporalstore_rust::raft::DataRaftConsensusBackend"
            .to_string(),
        openraft_dependency_removed: true,
        requirements: rustraft_requirements(),
    }
}

pub fn rustraft_public_api_contract() -> RustRaftPublicApiContract {
    RustRaftPublicApiContract {
        storage_trait: "RustRaftStorage".to_string(),
        transport_trait: "RaftTransport".to_string(),
        rpc_messages: vec![
            "AppendEntriesRequest".to_string(),
            "VoteRequest".to_string(),
            "InstallSnapshotRequest".to_string(),
            "ReadIndexRequest".to_string(),
            "AuthenticatedRaftRpc".to_string(),
        ],
        safety_helpers: vec![
            "rustraft_read_safety_decision".to_string(),
            "rustraft_append_safety_decision".to_string(),
            "rustraft_learner_promotion_decision".to_string(),
        ],
        metrics: rustraft_metric_names(),
    }
}

pub fn rustraft_metric_names() -> RustRaftMetricNames {
    RustRaftMetricNames {
        ready: "rustraft_ready".to_string(),
        append_latency_ms: "rustraft_append_latency_ms".to_string(),
        vote_latency_ms: "rustraft_vote_latency_ms".to_string(),
        read_index_latency_ms: "rustraft_read_index_latency_ms".to_string(),
        snapshot_install_latency_ms: "rustraft_snapshot_install_latency_ms".to_string(),
        peer_append_queue_depth: "rustraft_peer_append_queue_depth".to_string(),
        peer_reorder_queue_depth: "rustraft_peer_reorder_queue_depth".to_string(),
        peer_snapshot_installed_index: "rustraft_peer_snapshot_installed_index".to_string(),
        wal_segment_count: "rustraft_wal_segment_count".to_string(),
    }
}

pub fn rustraft_replication_health(
    status: &RustRaftStatusSnapshot,
    peer_pipeline: &[RaftPeerPipelineState],
) -> RaftReplicationHealth {
    let max_status_lag = status
        .peers
        .iter()
        .map(|peer| peer.lag)
        .max()
        .unwrap_or_default();
    let max_pipeline_lag = peer_pipeline
        .iter()
        .map(|peer| status.commit_index.saturating_sub(peer.match_index))
        .max()
        .unwrap_or_default();
    let max_peer_lag = max_status_lag.max(max_pipeline_lag);
    let lagging_peer_count = status.peers.iter().filter(|peer| peer.lag > 0).count() as u64
        + peer_pipeline
            .iter()
            .filter(|peer| peer.match_index < status.commit_index)
            .count() as u64;
    let replicated_peer_count = status.peers.iter().filter(|peer| peer.healthy).count() as u64;
    let status_value = if status.leader_id.is_none() {
        RaftHealthStatus::Unavailable
    } else if lagging_peer_count > 0 {
        RaftHealthStatus::Degraded
    } else {
        RaftHealthStatus::Healthy
    };
    RaftReplicationHealth {
        status: status_value,
        leader_id: status.leader_id,
        commit_index: status.commit_index,
        replicated_peer_count,
        lagging_peer_count,
        max_peer_lag,
        reason: match status_value {
            RaftHealthStatus::Healthy => "replication_healthy".to_string(),
            RaftHealthStatus::Degraded => "replication_lagging".to_string(),
            RaftHealthStatus::Unavailable => "leader_unavailable".to_string(),
        },
    }
}

pub fn rustraft_apply_health(status: &RustRaftStatusSnapshot) -> RaftApplyHealth {
    let apply_lag = status.commit_index.saturating_sub(status.applied_index);
    let status_value = if status.leader_id.is_none() {
        RaftHealthStatus::Unavailable
    } else if apply_lag > 0 {
        RaftHealthStatus::Degraded
    } else {
        RaftHealthStatus::Healthy
    };
    RaftApplyHealth {
        status: status_value,
        commit_index: status.commit_index,
        applied_index: status.applied_index,
        apply_lag,
        reason: match status_value {
            RaftHealthStatus::Healthy => "apply_healthy".to_string(),
            RaftHealthStatus::Degraded => "apply_lagging".to_string(),
            RaftHealthStatus::Unavailable => "leader_unavailable".to_string(),
        },
    }
}

pub fn rustraft_runtime_local_status_report(
    node_status: RustRaftStatusSnapshot,
    peer_pipeline: Vec<RaftPeerPipelineState>,
    readiness: RustRaftReadinessSnapshot,
) -> RaftRuntimeLocalStatusReport {
    let replication_health = rustraft_replication_health(&node_status, &peer_pipeline);
    let apply_health = rustraft_apply_health(&node_status);
    let mut blockers = Vec::new();
    if replication_health.status != RaftHealthStatus::Healthy {
        blockers.push(replication_health.reason.clone());
    }
    if apply_health.status != RaftHealthStatus::Healthy {
        blockers.push(apply_health.reason.clone());
    }
    if !readiness.rustraft_operator_observability_present {
        blockers.push("operator_observability_missing".to_string());
    }
    let ready = blockers.is_empty();
    RaftRuntimeLocalStatusReport {
        node_status,
        peer_pipeline,
        replication_health,
        apply_health,
        readiness,
        ready,
        blockers,
    }
}

pub fn rustraft_cluster_status_report(
    group_id: RustRaftGroupId,
    leader_id: Option<RustRaftNodeId>,
    nodes: Vec<RustRaftStatusSnapshot>,
) -> RaftClusterStatusReport {
    let representative = nodes
        .iter()
        .find(|node| Some(node.node_id) == leader_id)
        .or_else(|| nodes.first());
    let (replication_health, apply_health) = if let Some(status) = representative {
        (
            rustraft_replication_health(status, &[]),
            rustraft_apply_health(status),
        )
    } else {
        (
            RaftReplicationHealth {
                status: RaftHealthStatus::Unavailable,
                leader_id,
                commit_index: 0,
                replicated_peer_count: 0,
                lagging_peer_count: 0,
                max_peer_lag: 0,
                reason: "cluster_has_no_nodes".to_string(),
            },
            RaftApplyHealth {
                status: RaftHealthStatus::Unavailable,
                commit_index: 0,
                applied_index: 0,
                apply_lag: 0,
                reason: "cluster_has_no_nodes".to_string(),
            },
        )
    };
    let mut blockers = Vec::new();
    if leader_id.is_none() {
        blockers.push("leader_unavailable".to_string());
    }
    if replication_health.status != RaftHealthStatus::Healthy {
        blockers.push(replication_health.reason.clone());
    }
    if apply_health.status != RaftHealthStatus::Healthy {
        blockers.push(apply_health.reason.clone());
    }
    let health = if blockers.is_empty() {
        RaftHealthStatus::Healthy
    } else if leader_id.is_some() {
        RaftHealthStatus::Degraded
    } else {
        RaftHealthStatus::Unavailable
    };
    RaftClusterStatusReport {
        group_id,
        leader_id,
        nodes,
        replication_health,
        apply_health,
        ready: blockers.is_empty(),
        health,
        blockers,
    }
}

pub fn rustraft_capability_evidence(
    readiness: &RustRaftReadinessSnapshot,
) -> Vec<RaftCapabilityEvidence> {
    rustraft_readiness_evidence(readiness)
        .into_iter()
        .map(|evidence| RaftCapabilityEvidence {
            capability: evidence.requirement_id,
            present: evidence.present,
            evidence: vec![evidence.readiness_field],
            source_reference: "rustraft_readiness_snapshot".to_string(),
        })
        .collect()
}

pub fn rustraft_runtime_admin_report(
    cluster_status: RaftClusterStatusReport,
    readiness: RustRaftReadinessSnapshot,
    capability_evidence: Vec<RaftCapabilityEvidence>,
) -> RaftRuntimeAdminReport {
    let parity = rustraft_parity_report(&readiness);
    let public_api = rustraft_public_api_contract();
    let mut blockers = cluster_status.blockers.clone();
    blockers.extend(
        capability_evidence
            .iter()
            .filter(|evidence| !evidence.present)
            .map(|evidence| format!("capability_missing:{}", evidence.capability)),
    );
    blockers.extend(parity.production_blockers.iter().cloned());
    blockers.sort();
    blockers.dedup();
    let ready = cluster_status.ready
        && parity.ready
        && capability_evidence.iter().all(|evidence| evidence.present)
        && blockers.is_empty();
    let health = if ready {
        RaftHealthStatus::Healthy
    } else if cluster_status.leader_id.is_some() {
        RaftHealthStatus::Degraded
    } else {
        RaftHealthStatus::Unavailable
    };
    RaftRuntimeAdminReport {
        cluster_status,
        readiness,
        parity,
        public_api,
        capability_evidence,
        ready,
        health,
        blockers,
    }
}

pub fn rustraft_parity_report(snapshot: &RustRaftReadinessSnapshot) -> RustRaftParityReport {
    let contract = rustraft_parity_contract();
    let evidence = rustraft_readiness_evidence(snapshot);
    let satisfied = evidence
        .iter()
        .filter(|item| item.present)
        .map(|item| item.requirement_id.clone())
        .collect::<Vec<_>>();
    let missing = evidence
        .iter()
        .filter(|item| !item.present)
        .map(|item| item.requirement_id.clone())
        .collect::<Vec<_>>();
    let production_blockers = contract
        .requirements
        .iter()
        .filter(|requirement| {
            requirement.required_for_production && missing.iter().any(|id| id == &requirement.id)
        })
        .map(|requirement| format!("{:?}:{}", requirement.category, requirement.id).to_lowercase())
        .collect::<Vec<_>>();
    let byteraft_parity_matrix = rustraft_byteraft_parity_matrix(snapshot);
    let byteraft_gaps = byteraft_parity_matrix
        .iter()
        .filter(|item| item.status == RustRaftByteRaftParityStatus::Gap)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let byteraft_intentional_differences = byteraft_parity_matrix
        .iter()
        .filter(|item| item.status == RustRaftByteRaftParityStatus::IntentionalDifference)
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    let ready = missing.is_empty() && production_blockers.is_empty();
    RustRaftParityReport {
        contract,
        byteraft_reference_policy: rustraft_byteraft_reference_policy(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
        byteraft_parity_matrix,
        byteraft_gaps,
        byteraft_intentional_differences,
    }
}

pub fn rustraft_byteraft_reference_policy() -> RustRaftByteRaftReferencePolicy {
    RustRaftByteRaftReferencePolicy {
        feature_reference: "ByteRaft is the feature reference for Raft behavior parity.".to_string(),
        performance_reference:
            "ByteRaft is the performance reference; RustRaft parity requires p50/p99 latency and throughput within the configured threshold."
                .to_string(),
        rust_api_policy:
            "RustRaft may expose idiomatic Rust traits, request/response types, and error types instead of ByteRaft-shaped APIs."
                .to_string(),
        temporalstore_consumption_boundary:
            "TemporalStore consumption must remain stable through temporalstore_rust::raft::DataRaftConsensusBackend and adapter-owned codecs/apply/storage wiring."
                .to_string(),
    }
}

pub fn rustraft_byteraft_parity_matrix(
    snapshot: &RustRaftReadinessSnapshot,
) -> Vec<RustRaftByteRaftParityItem> {
    use RustRaftByteRaftParityStatus::*;

    fn item(
        id: &str,
        status: RustRaftByteRaftParityStatus,
        evidence: &[&str],
        note: &str,
    ) -> RustRaftByteRaftParityItem {
        RustRaftByteRaftParityItem {
            id: id.to_string(),
            required: true,
            status,
            evidence: evidence.iter().map(|field| (*field).to_string()).collect(),
            note: note.to_string(),
        }
    }

    fn status(ready: bool) -> RustRaftByteRaftParityStatus {
        if ready {
            Satisfied
        } else {
            Gap
        }
    }

    vec![
        item(
            "log_replication",
            status(
                snapshot.rustraft_leader_write_authority_present
                    && snapshot.rustraft_rpc_transport_contract_present,
            ),
            &[
                "rustraft_leader_write_authority_present",
                "rustraft_rpc_transport_contract_present",
            ],
            "leader-owned append path and append RPC contract are present",
        ),
        item(
            "leader_election",
            status(
                snapshot.rustraft_leader_write_authority_present
                    && snapshot.rustraft_metaserver_snapshot_floor_election_present,
            ),
            &[
                "rustraft_leader_write_authority_present",
                "rustraft_metaserver_snapshot_floor_election_present",
            ],
            "leader authority and snapshot-floor election safety are present",
        ),
        item(
            "pre_vote",
            status(snapshot.rustraft_rpc_transport_contract_present),
            &["rustraft_rpc_transport_contract_present", "RustRaftVoteRequest.pre_vote"],
            "pre-vote is represented in the vote RPC contract",
        ),
        item(
            "lease_read",
            status(snapshot.rustraft_leader_write_authority_present),
            &[
                "rustraft_leader_write_authority_present",
                "RustRaftReadIndexRequest.allow_lease_read",
            ],
            "lease reads are admitted only through leader/read-safety helpers",
        ),
        item(
            "read_index",
            status(snapshot.rustraft_operator_observability_present),
            &[
                "rustraft_operator_observability_present",
                "RustRaftReadIndexRequest",
                "RustRaftReadIndexResponse",
            ],
            "read-index request/response and metrics are part of the public contract",
        ),
        item(
            "membership_changes",
            status(snapshot.metaserver_membership_workflow_present),
            &["metaserver_membership_workflow_present"],
            "membership workflow evidence covers add/remove and joint changes",
        ),
        item(
            "learner_promotion",
            status(snapshot.learner_catchup_promotion_present),
            &["learner_catchup_promotion_present"],
            "learner catch-up and promotion decision helpers are present",
        ),
        item(
            "witness_quorum_behavior",
            Satisfied,
            &["RustRaftReplicaRole::Witness.participates_in_quorum"],
            "witnesses count for quorum but are not data-serving leaders",
        ),
        item(
            "log_compaction",
            status(
                snapshot.rustraft_compacted_entry_rejection_present
                    && snapshot.rustraft_log_retention_snapshot_trigger_present,
            ),
            &[
                "rustraft_compacted_entry_rejection_present",
                "rustraft_log_retention_snapshot_trigger_present",
            ],
            "compacted-entry rejection and snapshot-trigger retention evidence are present",
        ),
        item(
            "snapshot_trigger_install",
            status(
                snapshot.rustraft_log_retention_snapshot_trigger_present
                    && snapshot.rustraft_snapshot_tail_catchup_present
                    && snapshot.rustraft_snapshot_floor_log_matching_present,
            ),
            &[
                "rustraft_log_retention_snapshot_trigger_present",
                "rustraft_snapshot_tail_catchup_present",
                "rustraft_snapshot_floor_log_matching_present",
            ],
            "snapshot trigger, install/catch-up, and floor matching are present",
        ),
        item(
            "restart_recovery",
            status(
                snapshot.raft_storage_apply_fence_present
                    && snapshot.rustraft_apply_snapshot_fence_present,
            ),
            &[
                "raft_storage_apply_fence_present",
                "rustraft_apply_snapshot_fence_present",
            ],
            "WAL recovery is guarded by storage and apply/snapshot fences",
        ),
        item(
            "leader_transfer",
            IntentionalDifference,
            &["RustRaftConsensus::transfer_leader"],
            "RustRaft exposes the transfer contract; process validation is attached by the consuming runtime",
        ),
        item(
            "observability_status",
            status(snapshot.rustraft_operator_observability_present),
            &["rustraft_operator_observability_present", "RustRaftStatusSnapshot"],
            "status snapshots and metric names are part of the public contract",
        ),
    ]
}

pub fn rustraft_production_readiness_report(
    input: &RustRaftProductionReadinessInput,
) -> RustRaftProductionReadinessReport {
    let parity = rustraft_parity_report(&input.readiness);
    let mut satisfied = parity
        .satisfied
        .iter()
        .map(|id| format!("contract:{id}"))
        .collect::<Vec<_>>();
    let mut missing = parity
        .missing
        .iter()
        .map(|id| format!("contract:{id}"))
        .collect::<Vec<_>>();
    let mut production_blockers = parity.production_blockers.clone();
    let mut recommended_next_actions = Vec::new();

    if parity.ready {
        satisfied.push("contract:all_required_semantics".to_string());
    } else {
        recommended_next_actions.push(
            "fix RustRaft semantic contract/readiness gaps before production rollout".to_string(),
        );
    }

    require_option(
        "pipeline:evidence_present",
        input.peer_pipeline.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach per-peer pipeline evidence from the running RustRaft group",
    );
    if let Some(pipeline) = &input.peer_pipeline {
        for (present, id, action) in [
            (
                pipeline.per_peer_pipeline_state_present,
                "pipeline:per_peer_state",
                "export per-peer replication/apply pipeline state",
            ),
            (
                pipeline.append_backpressure_enforced,
                "pipeline:append_backpressure",
                "prove append queue backpressure under load",
            ),
            (
                pipeline.apply_backpressure_enforced,
                "pipeline:apply_backpressure",
                "prove apply queue backpressure under load",
            ),
            (
                pipeline.memory_replicate_bytes_enforced,
                "pipeline:memory_replicate_bytes",
                "prove max_memory_replicate_log_bytes enforcement",
            ),
            (
                pipeline.oversized_log_rejection_present,
                "pipeline:oversized_log_rejection",
                "prove oversized log entry rejection",
            ),
            (
                pipeline.out_of_order_append_handling_present,
                "pipeline:out_of_order_append_handling",
                "prove out-of-order append handling/rejection",
            ),
            (
                pipeline.reorder_queue_enabled,
                "pipeline:reorder_queue",
                "enable and prove reorder queue behavior",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_option(
        "snapshot:evidence_present",
        input.snapshot_lifecycle.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach snapshot send/install lifecycle evidence",
    );
    if let Some(snapshot) = &input.snapshot_lifecycle {
        for (present, id, action) in [
            (
                snapshot.sender_lifecycle_present,
                "snapshot:sender_lifecycle",
                "prove snapshot sender lifecycle",
            ),
            (
                snapshot.downloader_lifecycle_present,
                "snapshot:downloader_lifecycle",
                "prove snapshot downloader/install lifecycle",
            ),
            (
                snapshot.retry_backpressure_present,
                "snapshot:retry_backpressure",
                "prove snapshot retry/backpressure behavior",
            ),
            (
                snapshot.chunk_retry_present,
                "snapshot:chunk_retry",
                "prove snapshot chunk retry behavior",
            ),
            (
                snapshot.send_timeout_present,
                "snapshot:send_timeout",
                "prove snapshot send timeout behavior",
            ),
            (
                snapshot.rate_limit_present,
                "snapshot:rate_limit",
                "prove snapshot rate limiting",
            ),
            (
                snapshot.install_progress_present,
                "snapshot:install_progress",
                "export snapshot install progress",
            ),
            (
                snapshot.install_rollback_present,
                "snapshot:install_rollback",
                "prove snapshot install rollback",
            ),
            (
                snapshot.membership_change_present,
                "snapshot:membership_change",
                "prove snapshot behavior during membership change",
            ),
            (
                snapshot.rejoin_after_compacted_log_present,
                "snapshot:rejoin_after_compacted_log",
                "prove rejoin after compacted log",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_option(
        "wal:evidence_present",
        input.wal_lifecycle.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
        "attach WAL segment/range/backpressure evidence",
    );
    if let Some(wal) = &input.wal_lifecycle {
        for (present, id, action) in [
            (
                wal.segment_lifecycle_present,
                "wal:segment_lifecycle",
                "prove WAL segment lifecycle",
            ),
            (
                wal.retained_range_present,
                "wal:retained_range",
                "prove retained WAL range reporting",
            ),
            (
                wal.sequence_range_present,
                "wal:sequence_range",
                "prove WAL sequence range reporting",
            ),
            (
                wal.log_index_range_present,
                "wal:log_index_range",
                "prove WAL log-index range reporting",
            ),
            (
                wal.compaction_observed,
                "wal:compaction",
                "prove WAL compaction/released segment behavior",
            ),
            (
                wal.slow_fsync_backpressure_observed,
                "wal:slow_fsync_backpressure",
                "prove slow fsync backpressure behavior",
            ),
        ] {
            require_bool(
                present,
                id,
                &mut satisfied,
                &mut missing,
                &mut production_blockers,
                &mut recommended_next_actions,
                action,
            );
        }
    }

    require_data_node_rollout(
        input.data_node_rollout.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_meta_rollout(
        input.metaserver_rollout.as_ref(),
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );
    require_membership_transitions(
        &input.membership_transitions,
        &mut satisfied,
        &mut missing,
        &mut production_blockers,
        &mut recommended_next_actions,
    );

    let ready = missing.is_empty() && production_blockers.is_empty();
    RustRaftProductionReadinessReport {
        parity,
        public_api: rustraft_public_api_contract(),
        ready,
        production_status: if ready {
            RustRaftProductionStatus::ProductionReady
        } else {
            RustRaftProductionStatus::Blocked
        },
        satisfied,
        missing,
        production_blockers,
        recommended_next_actions,
    }
}

pub fn rustraft_membership_readiness_report(
    transitions: &[RustRaftMembershipTransitionEvidence],
) -> RustRaftMembershipReadinessReport {
    let required = [
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::Failover,
        ),
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        (
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::Failover,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::ScaleUp,
        ),
        (
            RustRaftMembershipScope::DataNode,
            RustRaftMembershipTransitionKind::ScaleDown,
        ),
    ];
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    let mut decisions = Vec::new();

    for (scope, transition) in required {
        let id = membership_transition_id(scope, transition);
        let Some(evidence) = transitions
            .iter()
            .find(|item| item.scope == scope && item.transition == transition)
        else {
            missing.push(format!("{id}:evidence_present"));
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: vec!["evidence_present".to_string()],
            });
            continue;
        };
        let transition_missing = rustraft_membership_transition_missing(evidence);
        if transition_missing.is_empty() {
            satisfied.push(id);
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: true,
                missing: Vec::new(),
            });
        } else {
            missing.extend(
                transition_missing
                    .iter()
                    .map(|requirement| format!("{id}:{requirement}")),
            );
            decisions.push(RustRaftMembershipTransitionDecision {
                scope,
                transition,
                ready: false,
                missing: transition_missing,
            });
        }
    }

    RustRaftMembershipReadinessReport {
        ready: missing.is_empty(),
        satisfied,
        missing,
        decisions,
    }
}

pub fn rustraft_membership_transition_missing(
    evidence: &RustRaftMembershipTransitionEvidence,
) -> Vec<String> {
    let mut missing = Vec::new();
    let before_majority = majority_size(evidence.before_voters.len());
    let after_majority = majority_size(evidence.after_voters.len());
    if evidence.before_voters.len() < 3 {
        missing.push("before_voter_quorum_size".to_string());
    }
    if evidence.after_voters.len() < 3 {
        missing.push("after_voter_quorum_size".to_string());
    }
    if evidence.commit_index_after < evidence.commit_index_before {
        missing.push("monotonic_commit_index".to_string());
    }
    if evidence.applied_index_after < evidence.commit_index_after {
        missing.push("apply_catches_commit".to_string());
    }
    if !evidence.old_majority_preserved {
        missing.push(format!("old_majority_preserved_{before_majority}"));
    }
    if !evidence.new_majority_reached {
        missing.push(format!("new_majority_reached_{after_majority}"));
    }
    if !evidence.stale_leader_rejected {
        missing.push("stale_leader_rejected".to_string());
    }
    if !evidence.read_index_validated_after {
        missing.push("read_index_after_transition".to_string());
    }
    if !evidence.write_validated_after {
        missing.push("write_after_transition".to_string());
    }
    if !evidence.snapshot_floor_preserved {
        missing.push("snapshot_floor_preserved".to_string());
    }
    if !evidence.secondary_replication_visible {
        missing.push("secondary_replication_visible".to_string());
    }
    if matches!(evidence.scope, RustRaftMembershipScope::Metaserver)
        && !evidence.scheduler_generation_advanced
    {
        missing.push("scheduler_generation_advanced".to_string());
    }
    match evidence.transition {
        RustRaftMembershipTransitionKind::Failover => {
            if evidence.leader_before.is_none() || evidence.leader_after.is_none() {
                missing.push("leader_before_after_present".to_string());
            }
            if evidence.leader_before == evidence.leader_after {
                missing.push("leader_changed_after_failover".to_string());
            }
            if evidence.failed_or_removed_nodes.is_empty() {
                missing.push("failed_node_recorded".to_string());
            }
        }
        RustRaftMembershipTransitionKind::ScaleUp => {
            if !evidence.joint_consensus_used {
                missing.push("joint_consensus_used".to_string());
            }
            if evidence.added_nodes.is_empty() {
                missing.push("added_node_recorded".to_string());
            }
            if evidence.after_voters.len() <= evidence.before_voters.len() {
                missing.push("voter_count_increased".to_string());
            }
            if evidence.caught_up_nodes.is_empty() {
                missing.push("learner_catchup_observed".to_string());
            }
        }
        RustRaftMembershipTransitionKind::ScaleDown => {
            if !evidence.joint_consensus_used {
                missing.push("joint_consensus_used".to_string());
            }
            if evidence.failed_or_removed_nodes.is_empty() {
                missing.push("removed_node_recorded".to_string());
            }
            if evidence.after_voters.len() >= evidence.before_voters.len() {
                missing.push("voter_count_decreased".to_string());
            }
        }
    }
    missing.extend(
        evidence
            .blockers
            .iter()
            .map(|blocker| format!("blocker:{blocker}")),
    );
    missing
}

pub fn rustraft_pipeline_evidence(
    peers: &[RustRaftPeerPipelineStatus],
    limits: RustRaftPipelineLimits,
) -> RustRaftPipelineEvidence {
    RustRaftPipelineEvidence {
        per_peer_pipeline_state_present: !peers.is_empty(),
        append_backpressure_enforced: peers.iter().any(|peer| {
            peer.append_queue_limit == limits.max_inflights_replicate
                && (peer.append_queue_max_depth >= peer.append_queue_limit
                    || peer.append_queue_depth >= peer.append_queue_limit)
        }),
        apply_backpressure_enforced: peers.iter().any(|peer| {
            peer.apply_inflight_limit == limits.max_inflights_apply_task
                && (peer.apply_backpressure_rejections > 0
                    || peer.apply_queue_max_depth >= peer.apply_inflight_limit)
        }),
        memory_replicate_bytes_enforced: peers.iter().any(|peer| {
            peer.inflight_bytes_limit == limits.max_memory_replicate_log_bytes
                && peer.memory_backpressure_rejections > 0
        }),
        oversized_log_rejection_present: peers.iter().any(|peer| peer.oversized_log_rejections > 0),
        out_of_order_append_handling_present: peers.iter().any(|peer| {
            peer.out_of_order_append_rejections > 0
                || peer.reorder_entries_rejected > 0
                || peer.reorder_entry_timeouts > 0
                || peer.reorder_dropped_packages > 0
        }),
        reorder_timeout_drop_present: peers
            .iter()
            .any(|peer| peer.reorder_entry_timeouts > 0 && peer.reorder_dropped_packages > 0),
        stale_term_rejection_present: peers.iter().any(|peer| peer.stale_term_rejections > 0),
        reorder_queue_enabled: limits.enable_reorder_queue
            && limits.reorder_window_size > 0
            && limits.reorder_timeout_us > 0
            && peers.iter().any(|peer| peer.reorder_queue_depth > 0),
    }
}

pub fn rustraft_snapshot_lifecycle_evidence(
    peers: &[RustRaftPeerPipelineStatus],
    send_snapshot_timeout_ms: u64,
    max_inflights_replicate: u64,
) -> RustRaftSnapshotLifecycleEvidence {
    RustRaftSnapshotLifecycleEvidence {
        sender_lifecycle_present: send_snapshot_timeout_ms > 0
            && peers
                .iter()
                .any(|peer| peer.snapshot_sending || peer.snapshot_send_attempts > 0),
        downloader_lifecycle_present: peers
            .iter()
            .any(|peer| peer.snapshot_installing || peer.snapshot_install_total_chunks > 0),
        retry_backpressure_present: peers.iter().any(|peer| {
            peer.snapshot_backpressure_rejections > 0
                || (max_inflights_replicate > 0
                    && peer.snapshot_send_attempts > max_inflights_replicate)
        }),
        chunk_retry_present: peers.iter().any(|peer| peer.snapshot_chunk_retry_count > 0),
        send_timeout_present: peers.iter().any(|peer| peer.snapshot_send_timeouts > 0),
        rate_limit_present: peers
            .iter()
            .any(|peer| peer.snapshot_rate_limit_rejections > 0),
        install_progress_present: peers.iter().any(|peer| {
            peer.snapshot_installed_index > 0 || peer.snapshot_install_progress_per_mille > 0
        }),
        install_rollback_present: peers
            .iter()
            .any(|peer| peer.snapshot_install_rolled_back > 0),
        membership_change_present: peers
            .iter()
            .any(|peer| peer.snapshot_during_membership_change),
        rejoin_after_compacted_log_present: peers
            .iter()
            .any(|peer| peer.snapshot_rejoin_after_compacted_log),
    }
}

pub fn rustraft_wal_lifecycle_evidence(
    status: &RustRaftWalLifecycleStatus,
) -> RustRaftWalLifecycleEvidence {
    RustRaftWalLifecycleEvidence {
        segment_lifecycle_present: status.segment_count > 0
            && status.active_segment_id >= status.first_retained_segment_id
            && status.last_retained_segment_id >= status.first_retained_segment_id,
        retained_range_present: status.first_retained_segment_id <= status.last_retained_segment_id,
        sequence_range_present: status.first_sequence <= status.last_sequence
            && status.total_records > 0,
        log_index_range_present: status.first_log_index <= status.last_log_index
            && status.last_log_index > 0,
        compaction_observed: status.released_segment_count > 0,
        slow_fsync_backpressure_observed: status.slow_fsync_backpressure_observed,
    }
}

pub fn rustraft_readiness_evidence(
    snapshot: &RustRaftReadinessSnapshot,
) -> Vec<RustRaftReadinessEvidence> {
    rustraft_requirements()
        .into_iter()
        .map(|requirement| RustRaftReadinessEvidence {
            present: readiness_field_present(snapshot, &requirement.readiness_field),
            requirement_id: requirement.id,
            readiness_field: requirement.readiness_field,
        })
        .collect()
}

pub fn rustraft_read_safety_decision(
    status: &RustRaftStatusSnapshot,
    request: &RustRaftReadIndexRequest,
) -> RustRaftReadSafetyDecision {
    if status.group_id != request.group_id {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "group_mismatch".to_string(),
        };
    }
    if !matches!(status.role, RustRaftRole::Leader) {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "not_leader".to_string(),
        };
    }
    if status.applied_index < request.min_commit_index {
        return RustRaftReadSafetyDecision {
            safe: false,
            read_index: status.commit_index,
            lease_read: false,
            reason: "apply_lag".to_string(),
        };
    }
    RustRaftReadSafetyDecision {
        safe: true,
        read_index: status.commit_index,
        lease_read: request.allow_lease_read,
        reason: "safe".to_string(),
    }
}

pub fn rustraft_read_safety_runtime_decision(
    input: RustRaftReadSafetyRuntimeInput,
) -> RustRaftReadSafetyRuntimeDecision {
    let is_follower = input.node_id != input.leader_id;
    if matches!(input.operation, RustRaftReadSafetyOperation::Write) {
        let stale_follower_write_rejected = is_follower;
        let minority_partition_write_rejected = !input.has_majority;
        let allowed = !stale_follower_write_rejected && !minority_partition_write_rejected;
        return RustRaftReadSafetyRuntimeDecision {
            allowed,
            read_index: input.leader_commit_index,
            reason: if allowed {
                "write_authority".to_string()
            } else if stale_follower_write_rejected {
                "not_leader".to_string()
            } else {
                "minority_partition".to_string()
            },
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected,
            healed_follower_catchup_observed: false,
        };
    }

    if !input.node_alive || !input.role_can_serve_data {
        return RustRaftReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: "node_unavailable".to_string(),
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    if matches!(
        input.operation,
        RustRaftReadSafetyOperation::ReadIndex | RustRaftReadSafetyOperation::LeaseRead
    ) && (!input.leader_lease_valid || !input.has_majority)
    {
        return RustRaftReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: if !input.leader_lease_valid {
                "stale_leader_lease".to_string()
            } else {
                "minority_partition".to_string()
            },
            stale_leader_lease_rejected: !input.leader_lease_valid,
            lagging_follower_read_rejected: false,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: !input.has_majority,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    let lag = input
        .leader_commit_index
        .saturating_sub(input.node_commit_index);
    let max_lag = if matches!(
        input.operation,
        RustRaftReadSafetyOperation::BoundedStaleRead
    ) {
        input.max_stale_index_lag
    } else {
        0
    };
    if lag > max_lag {
        return RustRaftReadSafetyRuntimeDecision {
            allowed: false,
            read_index: input.node_commit_index,
            reason: "replica_lagging".to_string(),
            stale_leader_lease_rejected: false,
            lagging_follower_read_rejected: is_follower,
            stale_follower_write_rejected: false,
            minority_partition_read_rejected: false,
            minority_partition_write_rejected: false,
            healed_follower_catchup_observed: false,
        };
    }

    RustRaftReadSafetyRuntimeDecision {
        allowed: true,
        read_index: input.node_commit_index,
        reason: "safe".to_string(),
        stale_leader_lease_rejected: false,
        lagging_follower_read_rejected: false,
        stale_follower_write_rejected: false,
        minority_partition_read_rejected: false,
        minority_partition_write_rejected: false,
        healed_follower_catchup_observed: is_follower
            && input.node_commit_index == input.leader_commit_index,
    }
}

pub fn rustraft_learner_promotion_decision(
    status: &RustRaftStatusSnapshot,
    learner_id: u64,
    max_lag: u64,
) -> RustRaftLearnerPromotionDecision {
    let Some(peer) = status.peers.iter().find(|peer| peer.node_id == learner_id) else {
        return RustRaftLearnerPromotionDecision {
            promotable: false,
            learner_id,
            learner_match_index: 0,
            required_match_index: status.commit_index.saturating_sub(max_lag),
            reason: "learner_missing".to_string(),
        };
    };
    let required_match_index = status.commit_index.saturating_sub(max_lag);
    let promotable = peer.learner && peer.healthy && peer.matched >= required_match_index;
    RustRaftLearnerPromotionDecision {
        promotable,
        learner_id,
        learner_match_index: peer.matched,
        required_match_index,
        reason: if promotable {
            "caught_up".to_string()
        } else {
            "not_caught_up".to_string()
        },
    }
}

pub fn rustraft_append_safety_decision(
    first_retained_log_index: u64,
    snapshot_index: u64,
    request: &RustRaftAppendEntriesRequest,
) -> RustRaftAppendSafetyDecision {
    let prev_index = request.prev_log_id.as_ref().map(|id| id.index).unwrap_or(0);
    if prev_index > 0 && prev_index < first_retained_log_index && prev_index <= snapshot_index {
        return RustRaftAppendSafetyDecision {
            accepted: false,
            rejected_compacted_entry: true,
            reason: "prev_log_compacted".to_string(),
        };
    }
    if request
        .entries
        .iter()
        .any(|entry| entry.log_id.index < first_retained_log_index)
    {
        return RustRaftAppendSafetyDecision {
            accepted: false,
            rejected_compacted_entry: true,
            reason: "entry_compacted".to_string(),
        };
    }
    RustRaftAppendSafetyDecision {
        accepted: true,
        rejected_compacted_entry: false,
        reason: "accepted".to_string(),
    }
}

fn rustraft_requirements() -> Vec<RustRaftSemanticRequirement> {
    use RustRaftRequirementCategory::*;
    [
        (
            "leader_write_authority",
            Safety,
            "rustraft_leader_write_authority_present",
        ),
        (
            "operator_observability",
            Observability,
            "rustraft_operator_observability_present",
        ),
        (
            "rpc_transport_contract",
            Transport,
            "rustraft_rpc_transport_contract_present",
        ),
        (
            "snapshot_trigger",
            Durability,
            "rustraft_log_retention_snapshot_trigger_present",
        ),
        (
            "apply_snapshot_fence",
            Durability,
            "rustraft_apply_snapshot_fence_present",
        ),
        (
            "storage_apply_fence",
            Durability,
            "raft_storage_apply_fence_present",
        ),
        (
            "snapshot_floor_log_matching",
            Durability,
            "rustraft_snapshot_floor_log_matching_present",
        ),
        (
            "snapshot_tail_catchup",
            Durability,
            "rustraft_snapshot_tail_catchup_present",
        ),
        (
            "compacted_entry_rejection",
            Safety,
            "rustraft_compacted_entry_rejection_present",
        ),
        (
            "metaserver_snapshot_floor_election",
            Safety,
            "rustraft_metaserver_snapshot_floor_election_present",
        ),
        (
            "learner_catchup_promotion",
            Membership,
            "learner_catchup_promotion_present",
        ),
        (
            "metaserver_membership_workflow",
            Membership,
            "metaserver_membership_workflow_present",
        ),
    ]
    .into_iter()
    .map(
        |(id, category, readiness_field)| RustRaftSemanticRequirement {
            id: id.to_string(),
            category,
            readiness_field: readiness_field.to_string(),
            required_for_production: true,
        },
    )
    .collect()
}

fn require_option<T>(
    id: &str,
    value: Option<&T>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    require_bool(
        value.is_some(),
        id,
        satisfied,
        missing,
        blockers,
        actions,
        action,
    );
}

fn require_bool(
    present: bool,
    id: &str,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
    action: &str,
) {
    if present {
        satisfied.push(id.to_string());
    } else {
        missing.push(id.to_string());
        blockers.push(id.to_string());
        actions.push(action.to_string());
    }
}

fn require_data_node_rollout(
    rollout: Option<&RustRaftDataNodeProcessRolloutReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "data_node:evidence_present",
        rollout,
        satisfied,
        missing,
        blockers,
        actions,
        "attach data-node process rollout evidence",
    );
    let Some(rollout) = rollout else {
        return;
    };
    for (present, id, action) in [
        (
            rollout.ready,
            "data_node:ready",
            "make data-node rollout ready",
        ),
        (
            rollout.blockers.is_empty(),
            "data_node:no_blockers",
            "clear data-node rollout blockers",
        ),
        (
            !rollout.nodes.is_empty()
                && rollout.spawned_process_count as usize >= rollout.nodes.len(),
            "data_node:processes_spawned",
            "spawn and observe all data-node RustRaft processes",
        ),
        (
            !rollout.voters.is_empty(),
            "data_node:voters_present",
            "run data-node RustRaft with voter membership",
        ),
        (
            rollout.independent_wal_dirs,
            "data_node:independent_wal_dirs",
            "use independent WAL dirs per data-node process",
        ),
        (
            rollout.independent_snapshot_dirs,
            "data_node:independent_snapshot_dirs",
            "use independent snapshot dirs per data-node process",
        ),
        (
            rollout.write_proposed_through_process_api,
            "data_node:process_write_path",
            "prove writes enter through the process API",
        ),
        (
            rollout.read_index_responses_observed > 0,
            "data_node:read_index",
            "observe data-node read-index responses",
        ),
        (
            rollout.leader_transfer_validated,
            "data_node:leader_transfer",
            "validate data-node leader transfer",
        ),
        (
            rollout.failover_validated,
            "data_node:failover",
            "validate data-node failover",
        ),
        (
            rollout.membership_change_validated,
            "data_node:membership_change",
            "validate data-node membership add/promote/remove",
        ),
        (
            rollout.follower_lag_validated,
            "data_node:follower_lag",
            "validate data-node follower lag handling",
        ),
        (
            rollout.secondary_read_validated,
            "data_node:secondary_read",
            "validate data-node secondary read eligibility",
        ),
        (
            rollout.recovered_after_restart && rollout.restart_recovery_validated,
            "data_node:restart_recovery",
            "validate data-node restart recovery",
        ),
        (
            rollout.snapshot_install_validated,
            "data_node:snapshot_install",
            "validate data-node snapshot install",
        ),
        (
            rollout.applied_fence_validated,
            "data_node:apply_fence",
            "validate data-node apply fence",
        ),
        (
            rollout.multi_process_log_store_validated,
            "data_node:multi_process_log_store",
            "validate independent multi-process log stores",
        ),
        (
            rollout.operational_semantics.proves_runtime_semantics(),
            "data_node:operational_semantics",
            "prove data-node runtime semantics, not only API presence",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for missing_requirement in rollout.operational_semantics.missing_requirements() {
        require_bool(
            false,
            &format!("data_node:semantics:{missing_requirement}"),
            satisfied,
            missing,
            blockers,
            actions,
            "complete data-node operational semantics evidence",
        );
    }
    for blocker in &rollout.blockers {
        require_bool(
            false,
            &format!("data_node:blocker:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear data-node rollout blocker",
        );
    }
}

fn require_meta_rollout(
    rollout: Option<&RustRaftMetaProcessRolloutReport>,
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    require_option(
        "metaserver:evidence_present",
        rollout,
        satisfied,
        missing,
        blockers,
        actions,
        "attach metaserver process rollout evidence",
    );
    let Some(rollout) = rollout else {
        return;
    };
    for (present, id, action) in [
        (
            rollout.ready,
            "metaserver:ready",
            "make metaserver rollout ready",
        ),
        (
            rollout.blockers.is_empty(),
            "metaserver:no_blockers",
            "clear metaserver rollout blockers",
        ),
        (
            !rollout.nodes.is_empty()
                && rollout.spawned_process_count as usize >= rollout.nodes.len(),
            "metaserver:processes_spawned",
            "spawn and observe all metaserver RustRaft processes",
        ),
        (
            !rollout.voters.is_empty(),
            "metaserver:voters_present",
            "run metaserver RustRaft with voter membership",
        ),
        (
            rollout.independent_wal_dirs,
            "metaserver:independent_wal_dirs",
            "use independent WAL dirs per metaserver process",
        ),
        (
            rollout.independent_snapshot_dirs,
            "metaserver:independent_snapshot_dirs",
            "use independent snapshot dirs per metaserver process",
        ),
        (
            rollout.mutation_proposed_through_process_api,
            "metaserver:process_mutation_path",
            "prove metaserver mutations enter through the process API",
        ),
        (
            rollout.read_index_responses_observed > 0 && rollout.read_index_validated,
            "metaserver:read_index",
            "validate metaserver read-index responses",
        ),
        (
            rollout.applied_raft_mutations > 0,
            "metaserver:applied_mutations",
            "observe applied metaserver RustRaft mutations",
        ),
        (
            rollout.scheduler_task_replay_validated,
            "metaserver:scheduler_replay",
            "validate scheduler task replay from RustRaft log",
        ),
        (
            rollout.data_node_membership_results_ready
                && rollout.data_node_membership_workflow_report_attached
                && rollout.data_node_raft_group_results_observed,
            "metaserver:data_node_membership_workflow",
            "validate data-node membership workflow through metaserver RustRaft",
        ),
        (
            rollout.failover_validated,
            "metaserver:failover",
            "validate metaserver failover",
        ),
        (
            rollout.membership_change_validated,
            "metaserver:membership_change",
            "validate metaserver membership change",
        ),
        (
            rollout.follower_lag_validated,
            "metaserver:follower_lag",
            "validate metaserver follower lag handling",
        ),
        (
            rollout.secondary_read_validated,
            "metaserver:secondary_read",
            "validate metaserver secondary read eligibility",
        ),
        (
            rollout.recovered_after_restart,
            "metaserver:restart_recovery",
            "validate metaserver restart recovery",
        ),
        (
            rollout.snapshot_install_validated,
            "metaserver:snapshot_install",
            "validate metaserver snapshot install",
        ),
        (
            rollout.multi_process_log_store_validated,
            "metaserver:multi_process_log_store",
            "validate independent metaserver log stores",
        ),
        (
            rollout.operational_semantics.proves_runtime_semantics(),
            "metaserver:operational_semantics",
            "prove metaserver runtime semantics, not only API presence",
        ),
    ] {
        require_bool(present, id, satisfied, missing, blockers, actions, action);
    }
    for missing_requirement in rollout.operational_semantics.missing_requirements() {
        require_bool(
            false,
            &format!("metaserver:semantics:{missing_requirement}"),
            satisfied,
            missing,
            blockers,
            actions,
            "complete metaserver operational semantics evidence",
        );
    }
    for blocker in &rollout.blockers {
        require_bool(
            false,
            &format!("metaserver:blocker:{blocker}"),
            satisfied,
            missing,
            blockers,
            actions,
            "clear metaserver rollout blocker",
        );
    }
}

fn require_membership_transitions(
    transitions: &[RustRaftMembershipTransitionEvidence],
    satisfied: &mut Vec<String>,
    missing: &mut Vec<String>,
    blockers: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    let report = rustraft_membership_readiness_report(transitions);
    if report.ready {
        satisfied.push("membership:all_required_transitions".to_string());
    } else {
        missing.extend(report.missing.iter().map(|id| format!("membership:{id}")));
        blockers.extend(report.missing.iter().map(|id| format!("membership:{id}")));
        actions.push(
            "run metaserver and data-node RustRaft failover, scale-up, and scale-down transitions"
                .to_string(),
        );
    }
    for id in report.satisfied {
        satisfied.push(format!("membership:{id}"));
    }
}

fn membership_transition_id(
    scope: RustRaftMembershipScope,
    transition: RustRaftMembershipTransitionKind,
) -> String {
    format!("{scope:?}:{transition:?}").to_lowercase()
}

fn majority_size(voters: usize) -> usize {
    voters / 2 + 1
}

fn readiness_field_present(snapshot: &RustRaftReadinessSnapshot, field: &str) -> bool {
    match field {
        "rustraft_leader_write_authority_present" => {
            snapshot.rustraft_leader_write_authority_present
        }
        "rustraft_operator_observability_present" => {
            snapshot.rustraft_operator_observability_present
        }
        "rustraft_rpc_transport_contract_present" => {
            snapshot.rustraft_rpc_transport_contract_present
        }
        "rustraft_log_retention_snapshot_trigger_present" => {
            snapshot.rustraft_log_retention_snapshot_trigger_present
        }
        "rustraft_apply_snapshot_fence_present" => snapshot.rustraft_apply_snapshot_fence_present,
        "raft_storage_apply_fence_present" => snapshot.raft_storage_apply_fence_present,
        "rustraft_snapshot_floor_log_matching_present" => {
            snapshot.rustraft_snapshot_floor_log_matching_present
        }
        "rustraft_snapshot_tail_catchup_present" => snapshot.rustraft_snapshot_tail_catchup_present,
        "rustraft_compacted_entry_rejection_present" => {
            snapshot.rustraft_compacted_entry_rejection_present
        }
        "rustraft_metaserver_snapshot_floor_election_present" => {
            snapshot.rustraft_metaserver_snapshot_floor_election_present
        }
        "learner_catchup_promotion_present" => snapshot.learner_catchup_promotion_present,
        "metaserver_membership_workflow_present" => snapshot.metaserver_membership_workflow_present,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_snapshot() -> RustRaftReadinessSnapshot {
        RustRaftReadinessSnapshot {
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
        }
    }

    fn ready_operational_semantics() -> RustRaftProcessOperationalSemanticsEvidence {
        RustRaftProcessOperationalSemanticsEvidence {
            api_presence_only_rejected: true,
            process_path_validated: true,
            read_index_validated: true,
            leader_lease_validated: true,
            stale_leader_lease_rejection_observed: true,
            follower_lease_expiration_observed: true,
            lagging_follower_read_rejected: true,
            bounded_stale_read_acceptance_observed: true,
            bounded_stale_read_rejection_observed: true,
            minority_partition_read_rejection_observed: true,
            healed_follower_catchup_observed: true,
            stale_follower_write_rejected: true,
            leader_transfer_exact_once_validated: true,
            leader_transfer_under_load_validated: true,
            snapshot_bootstrap_validated: true,
            snapshot_install_restart_validated: true,
            membership_rescale_validated: true,
            membership_add_promote_remove_validated: true,
            follower_rejoin_after_compaction_validated: true,
            secondary_read_eligibility_validated: true,
            apply_pipeline_converged: true,
            wal_persistence_observed: true,
            fsm_apply_idempotent_replay_observed: true,
            storage_mutation_wal_fence_atomicity_observed: true,
            snapshot_install_apply_fence_atomicity_observed: true,
            process_restart_after_apply_crash_recovered: true,
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn ready_process_nodes() -> Vec<RustRaftProcessNodeEvidence> {
        vec![
            RustRaftProcessNodeEvidence {
                node_id: 1,
                addr: "127.0.0.1:19001".to_string(),
                wal_dir: "/tmp/rustraft/node1/wal".to_string(),
                snapshot_dir: "/tmp/rustraft/node1/snapshots".to_string(),
                commit_index: 42,
                applied_index: 42,
                snapshot_id: Some("snap-40".to_string()),
                restarted: true,
                log_store_validated: true,
            },
            RustRaftProcessNodeEvidence {
                node_id: 2,
                addr: "127.0.0.1:19002".to_string(),
                wal_dir: "/tmp/rustraft/node2/wal".to_string(),
                snapshot_dir: "/tmp/rustraft/node2/snapshots".to_string(),
                commit_index: 42,
                applied_index: 42,
                snapshot_id: Some("snap-40".to_string()),
                restarted: true,
                log_store_validated: true,
            },
        ]
    }

    fn ready_data_node_rollout() -> RustRaftDataNodeProcessRolloutReport {
        RustRaftDataNodeProcessRolloutReport {
            shard_id: 7,
            voters: vec![1, 2, 3],
            learners: vec![4],
            nodes: ready_process_nodes(),
            spawned_process_count: 2,
            independent_wal_dirs: true,
            independent_snapshot_dirs: true,
            observed_process_requests: 16,
            read_index_responses_observed: 8,
            restarted_node_count: 2,
            per_node_log_store_inspection_count: 2,
            write_proposed_through_process_api: true,
            leader_transfer_validated: true,
            failover_validated: true,
            secondary_lag_observed: true,
            lagging_follower_read_rejection_observed: true,
            stale_follower_write_rejection_observed: true,
            catchup_read_eligibility_observed: true,
            minority_partition_rejection_observed: true,
            bounded_stale_read_eligibility_observed: true,
            healed_follower_catchup_observed: true,
            lagging_follower_observed_lag: 3,
            membership_change_validated: true,
            follower_lag_validated: true,
            secondary_read_validated: true,
            recovered_after_restart: true,
            restart_recovery_validated: true,
            snapshot_install_validated: true,
            applied_fence_validated: true,
            crash_after_storage_mutation_recovered: true,
            crash_after_wal_persist_recovered: true,
            crash_during_snapshot_install_recovered: true,
            apply_fence_recovered_after_restart: true,
            multi_process_log_store_validated: true,
            operational_semantics: ready_operational_semantics(),
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn ready_meta_rollout() -> RustRaftMetaProcessRolloutReport {
        RustRaftMetaProcessRolloutReport {
            voters: vec![1, 2, 3],
            learners: vec![4],
            nodes: ready_process_nodes(),
            spawned_process_count: 2,
            independent_wal_dirs: true,
            independent_snapshot_dirs: true,
            observed_process_requests: 20,
            read_index_responses_observed: 10,
            restarted_node_count: 2,
            per_node_log_store_inspection_count: 2,
            mutation_proposed_through_process_api: true,
            applied_raft_mutations: 12,
            generated_scheduler_tasks: 4,
            scheduler_retries: 1,
            stale_scheduler_token_rejected: true,
            data_node_membership_results_ready: true,
            scheduler_mutations_proposed_through_process_api: true,
            scheduler_task_replay_from_raft_log_observed: true,
            membership_mutations_proposed_through_process_api: true,
            data_node_membership_workflow_report_attached: true,
            data_node_raft_group_results_observed: true,
            failover_validated: true,
            membership_change_validated: true,
            follower_lag_validated: true,
            secondary_read_validated: true,
            read_index_validated: true,
            snapshot_install_validated: true,
            recovered_after_restart: true,
            scheduler_task_replay_validated: true,
            crash_after_meta_mutation_recovered: true,
            crash_after_meta_wal_persist_recovered: true,
            crash_during_meta_snapshot_install_recovered: true,
            meta_apply_fence_recovered_after_restart: true,
            multi_process_log_store_validated: true,
            operational_semantics: ready_operational_semantics(),
            ready: true,
            blockers: Vec::new(),
        }
    }

    fn membership_transition(
        scope: RustRaftMembershipScope,
        transition: RustRaftMembershipTransitionKind,
    ) -> RustRaftMembershipTransitionEvidence {
        match transition {
            RustRaftMembershipTransitionKind::Failover => RustRaftMembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3],
                after_voters: vec![1, 2, 3],
                before_learners: Vec::new(),
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(2),
                failed_or_removed_nodes: vec![1],
                added_nodes: Vec::new(),
                caught_up_nodes: vec![2, 3],
                commit_index_before: 100,
                commit_index_after: 104,
                applied_index_after: 104,
                joint_consensus_used: false,
                old_majority_preserved: true,
                new_majority_reached: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
                blockers: Vec::new(),
            },
            RustRaftMembershipTransitionKind::ScaleUp => RustRaftMembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3],
                after_voters: vec![1, 2, 3, 4],
                before_learners: vec![4],
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(1),
                failed_or_removed_nodes: Vec::new(),
                added_nodes: vec![4],
                caught_up_nodes: vec![4],
                commit_index_before: 100,
                commit_index_after: 108,
                applied_index_after: 108,
                joint_consensus_used: true,
                old_majority_preserved: true,
                new_majority_reached: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
                blockers: Vec::new(),
            },
            RustRaftMembershipTransitionKind::ScaleDown => RustRaftMembershipTransitionEvidence {
                scope,
                transition,
                before_voters: vec![1, 2, 3, 4],
                after_voters: vec![1, 2, 3],
                before_learners: Vec::new(),
                after_learners: Vec::new(),
                leader_before: Some(1),
                leader_after: Some(1),
                failed_or_removed_nodes: vec![4],
                added_nodes: Vec::new(),
                caught_up_nodes: vec![1, 2, 3],
                commit_index_before: 108,
                commit_index_after: 112,
                applied_index_after: 112,
                joint_consensus_used: true,
                old_majority_preserved: true,
                new_majority_reached: true,
                stale_leader_rejected: true,
                read_index_validated_after: true,
                write_validated_after: true,
                snapshot_floor_preserved: true,
                secondary_replication_visible: true,
                scheduler_generation_advanced: matches!(scope, RustRaftMembershipScope::Metaserver),
                blockers: Vec::new(),
            },
        }
    }

    fn ready_membership_transitions() -> Vec<RustRaftMembershipTransitionEvidence> {
        [
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipScope::DataNode,
        ]
        .into_iter()
        .flat_map(|scope| {
            [
                RustRaftMembershipTransitionKind::Failover,
                RustRaftMembershipTransitionKind::ScaleUp,
                RustRaftMembershipTransitionKind::ScaleDown,
            ]
            .into_iter()
            .map(move |transition| membership_transition(scope, transition))
        })
        .collect()
    }

    fn ready_production_input() -> RustRaftProductionReadinessInput {
        RustRaftProductionReadinessInput {
            readiness: ready_snapshot(),
            peer_pipeline: Some(RustRaftPipelineEvidence {
                per_peer_pipeline_state_present: true,
                append_backpressure_enforced: true,
                apply_backpressure_enforced: true,
                memory_replicate_bytes_enforced: true,
                oversized_log_rejection_present: true,
                out_of_order_append_handling_present: true,
                reorder_timeout_drop_present: true,
                stale_term_rejection_present: true,
                reorder_queue_enabled: true,
            }),
            snapshot_lifecycle: Some(RustRaftSnapshotLifecycleEvidence {
                sender_lifecycle_present: true,
                downloader_lifecycle_present: true,
                retry_backpressure_present: true,
                chunk_retry_present: true,
                send_timeout_present: true,
                rate_limit_present: true,
                install_progress_present: true,
                install_rollback_present: true,
                membership_change_present: true,
                rejoin_after_compacted_log_present: true,
            }),
            wal_lifecycle: Some(RustRaftWalLifecycleEvidence {
                segment_lifecycle_present: true,
                retained_range_present: true,
                sequence_range_present: true,
                log_index_range_present: true,
                compaction_observed: true,
                slow_fsync_backpressure_observed: true,
            }),
            data_node_rollout: Some(ready_data_node_rollout()),
            metaserver_rollout: Some(ready_meta_rollout()),
            membership_transitions: ready_membership_transitions(),
        }
    }

    #[test]
    fn contract_is_openraft_free_and_complete() {
        let contract = rustraft_parity_contract();
        assert!(contract.openraft_dependency_removed);
        assert_eq!(contract.requirements.len(), 12);
    }

    #[test]
    fn crate_readme_documents_open_source_contract_surface() {
        let readme = include_str!("../README.md");
        assert!(readme.contains("RustRaft"));
        assert!(readme.contains("rustraft_parity_report"));
        assert!(readme.contains("rustraft_production_readiness_report"));
        assert!(readme.contains("OpenRaft-free"));
        assert!(readme.contains("Apache-2.0"));
    }

    #[test]
    fn report_fails_closed() {
        let mut snapshot = ready_snapshot();
        snapshot.raft_storage_apply_fence_present = false;
        let report = rustraft_parity_report(&snapshot);
        assert!(!report.ready);
        assert_eq!(report.production_status, RustRaftProductionStatus::Blocked);
        assert_eq!(report.missing, vec!["storage_apply_fence".to_string()]);
    }

    #[test]
    fn production_readiness_gate_accepts_complete_evidence() {
        let report = rustraft_production_readiness_report(&ready_production_input());
        assert!(report.ready, "{report:#?}");
        assert_eq!(
            report.production_status,
            RustRaftProductionStatus::ProductionReady
        );
        assert!(report.missing.is_empty());
        assert!(report.production_blockers.is_empty());
        assert_eq!(report.public_api.storage_trait, "RustRaftStorage");
    }

    #[test]
    fn production_readiness_gate_fails_closed_without_runtime_evidence() {
        let report = rustraft_production_readiness_report(&RustRaftProductionReadinessInput {
            readiness: ready_snapshot(),
            peer_pipeline: None,
            snapshot_lifecycle: None,
            wal_lifecycle: None,
            data_node_rollout: None,
            metaserver_rollout: None,
            membership_transitions: Vec::new(),
        });
        assert!(!report.ready);
        assert_eq!(report.production_status, RustRaftProductionStatus::Blocked);
        assert!(report
            .missing
            .contains(&"pipeline:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"snapshot:evidence_present".to_string()));
        assert!(report.missing.contains(&"wal:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"data_node:evidence_present".to_string()));
        assert!(report
            .missing
            .contains(&"metaserver:evidence_present".to_string()));
        assert!(report
            .missing
            .iter()
            .any(|item| item == "membership:datanode:scaledown:evidence_present"));
    }

    #[test]
    fn production_readiness_gate_reports_specific_wal_blocker() {
        let mut input = ready_production_input();
        input.wal_lifecycle = Some(RustRaftWalLifecycleEvidence {
            segment_lifecycle_present: true,
            retained_range_present: true,
            sequence_range_present: true,
            log_index_range_present: true,
            compaction_observed: false,
            slow_fsync_backpressure_observed: true,
        });
        let report = rustraft_production_readiness_report(&input);
        assert!(!report.ready);
        assert!(report.missing.contains(&"wal:compaction".to_string()));
        assert!(report
            .recommended_next_actions
            .contains(&"prove WAL compaction/released segment behavior".to_string()));
    }

    #[test]
    fn safety_helpers_accept_healthy_state() {
        let status = RustRaftStatusSnapshot {
            group_id: 1,
            node_id: 1,
            role: RustRaftRole::Leader,
            term: 2,
            leader_id: Some(1),
            commit_index: 10,
            applied_index: 10,
            last_log_index: 10,
            last_snapshot_index: 4,
            peers: vec![RustRaftPeerStatus {
                node_id: 2,
                matched: 10,
                next_index: 11,
                learner: true,
                healthy: true,
                lag: 0,
            }],
        };
        assert!(
            rustraft_read_safety_decision(
                &status,
                &RustRaftReadIndexRequest {
                    group_id: 1,
                    requester_id: 1,
                    min_commit_index: 10,
                    allow_lease_read: true,
                },
            )
            .safe
        );
        assert!(rustraft_learner_promotion_decision(&status, 2, 0).promotable);
    }

    #[test]
    fn runtime_read_safety_rejects_stale_leader_lease() {
        let decision = rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
            operation: RustRaftReadSafetyOperation::ReadIndex,
            node_id: 1,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: false,
            has_majority: true,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        });
        assert!(!decision.allowed);
        assert!(decision.stale_leader_lease_rejected);
        assert_eq!(decision.reason, "stale_leader_lease");
    }

    #[test]
    fn runtime_read_safety_rejects_lagging_follower() {
        let decision = rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
            operation: RustRaftReadSafetyOperation::ReadIndex,
            node_id: 2,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: true,
            node_commit_index: 7,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        });
        assert!(!decision.allowed);
        assert!(decision.lagging_follower_read_rejected);
        assert_eq!(decision.reason, "replica_lagging");
    }

    #[test]
    fn runtime_read_safety_allows_bounded_stale_within_lag_budget() {
        let decision = rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
            operation: RustRaftReadSafetyOperation::BoundedStaleRead,
            node_id: 2,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: true,
            node_commit_index: 8,
            leader_commit_index: 10,
            max_stale_index_lag: 2,
        });
        assert!(decision.allowed);
        assert_eq!(decision.read_index, 8);
    }

    #[test]
    fn runtime_read_safety_rejects_minority_writes() {
        let decision = rustraft_read_safety_runtime_decision(RustRaftReadSafetyRuntimeInput {
            operation: RustRaftReadSafetyOperation::Write,
            node_id: 1,
            leader_id: 1,
            node_alive: true,
            role_can_serve_data: true,
            leader_lease_valid: true,
            has_majority: false,
            node_commit_index: 10,
            leader_commit_index: 10,
            max_stale_index_lag: 0,
        });
        assert!(!decision.allowed);
        assert!(decision.minority_partition_write_rejected);
        assert_eq!(decision.reason, "minority_partition");
    }

    #[test]
    fn membership_readiness_requires_failover_scale_up_and_scale_down_for_meta_and_data_nodes() {
        let report = rustraft_membership_readiness_report(&ready_membership_transitions());
        assert!(report.ready, "{report:#?}");
        assert!(report
            .satisfied
            .contains(&"metaserver:failover".to_string()));
        assert!(report.satisfied.contains(&"metaserver:scaleup".to_string()));
        assert!(report
            .satisfied
            .contains(&"metaserver:scaledown".to_string()));
        assert!(report.satisfied.contains(&"datanode:failover".to_string()));
        assert!(report.satisfied.contains(&"datanode:scaleup".to_string()));
        assert!(report.satisfied.contains(&"datanode:scaledown".to_string()));
    }

    #[test]
    fn membership_readiness_fails_closed_when_transition_evidence_is_missing() {
        let transitions = ready_membership_transitions()
            .into_iter()
            .filter(|item| {
                !(item.scope == RustRaftMembershipScope::DataNode
                    && item.transition == RustRaftMembershipTransitionKind::ScaleDown)
            })
            .collect::<Vec<_>>();
        let report = rustraft_membership_readiness_report(&transitions);
        assert!(!report.ready);
        assert!(report
            .missing
            .contains(&"datanode:scaledown:evidence_present".to_string()));
    }

    #[test]
    fn membership_readiness_rejects_unsafe_scale_up_without_joint_consensus() {
        let mut transition = membership_transition(
            RustRaftMembershipScope::Metaserver,
            RustRaftMembershipTransitionKind::ScaleUp,
        );
        transition.joint_consensus_used = false;
        let missing = rustraft_membership_transition_missing(&transition);
        assert!(missing.contains(&"joint_consensus_used".to_string()));
    }
}
