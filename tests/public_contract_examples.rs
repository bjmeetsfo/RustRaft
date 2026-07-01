use rustraft::{
    rustraft_parity_report, rustraft_read_safety_decision, RustRaftProductionStatus,
    RustRaftReadIndexRequest, RustRaftReadinessSnapshot, RustRaftRole, RustRaftStatusSnapshot,
};

#[test]
fn production_readiness_snapshot_reports_ready_when_all_evidence_is_present() {
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
    assert!(report.missing.is_empty());
    assert_eq!(
        report.production_status,
        RustRaftProductionStatus::ProductionReady
    );
}

#[test]
fn read_safety_example_rejects_reads_ahead_of_applied_index() {
    let status = RustRaftStatusSnapshot {
        group_id: 7,
        node_id: 1,
        role: RustRaftRole::Leader,
        term: 9,
        leader_id: Some(1),
        commit_index: 42,
        applied_index: 42,
        last_log_index: 42,
        last_snapshot_index: 30,
        peers: Vec::new(),
    };

    let decision = rustraft_read_safety_decision(
        &status,
        &RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 43,
            allow_lease_read: true,
        },
    );

    assert!(!decision.safe);
    assert_eq!(decision.reason, "apply_lag");
}
