//! RustRaft readiness and parity contract types.
//!
//! This crate deliberately keeps the public RustRaft contract separate from
//! TemporalStore's data-node and metaserver runtime code. Runtime crates provide
//! readiness evidence; this crate turns that evidence into a stable parity
//! contract and report.

use serde::{Deserialize, Serialize};

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
}
