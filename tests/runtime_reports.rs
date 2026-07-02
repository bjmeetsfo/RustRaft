use rustraft::{
    rustraft_capability_evidence, rustraft_runtime_admin_report,
    rustraft_runtime_local_status_report, RaftCluster, RaftHealthStatus, RaftPeerPipelineState,
    RustRaftPeer, RustRaftReplicaRole, RustRaftRole, RustRaftStatusSnapshot,
};

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 6_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 7_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn ready_snapshot() -> rustraft::RustRaftReadinessSnapshot {
    rustraft::RustRaftReadinessSnapshot {
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

#[test]
fn local_status_report_tracks_replication_apply_and_pipeline_health() {
    let status = RustRaftStatusSnapshot {
        group_id: 5,
        node_id: 1,
        role: RustRaftRole::Leader,
        term: 3,
        leader_id: Some(1),
        commit_index: 10,
        applied_index: 9,
        last_log_index: 10,
        last_snapshot_index: 4,
        peers: Vec::new(),
    };
    let pipeline = vec![RaftPeerPipelineState {
        peer_id: 2,
        match_index: 8,
        next_index: 9,
        append_requests: 10,
        append_accepted: 8,
        append_rejected: 2,
        inflight_entries: 1,
        inflight_bytes: 64,
        append_queue_depth: 1,
        append_queue_limit: 16,
        append_queue_max_depth: 2,
        inflight_bytes_limit: 1024,
        apply_inflight_tasks: 1,
        apply_inflight_limit: 8,
        apply_queue_depth: 1,
        apply_queue_max_depth: 2,
        apply_batch_bytes_limit: 1024,
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
        snapshot_installed_index: 4,
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
    }];

    let report = rustraft_runtime_local_status_report(status, pipeline, ready_snapshot());
    assert_eq!(report.replication_health.status, RaftHealthStatus::Degraded);
    assert_eq!(report.apply_health.status, RaftHealthStatus::Degraded);
    assert!(report.blockers.contains(&"replication_lagging".to_string()));
    assert!(report.blockers.contains(&"apply_lagging".to_string()));
}

#[test]
fn cluster_status_report_is_derived_from_runtime_cluster() {
    let mut cluster =
        RaftCluster::new(5, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("write");

    let report = cluster.cluster_status_report().expect("cluster status");
    assert_eq!(report.group_id, 5);
    assert_eq!(report.health, RaftHealthStatus::Healthy);
    assert!(report.ready);
    assert_eq!(report.nodes.len(), 3);
}

#[test]
fn admin_report_genericizes_byteraft_parity_evidence_for_rustraft() {
    let mut cluster =
        RaftCluster::new(5, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("write");
    let readiness = ready_snapshot();
    let capability_evidence = rustraft_capability_evidence(&readiness);
    let report = rustraft_runtime_admin_report(
        cluster.cluster_status_report().expect("cluster status"),
        readiness,
        capability_evidence,
    );

    assert!(report.ready);
    assert_eq!(report.health, RaftHealthStatus::Healthy);
    assert_eq!(report.public_api.transport_trait, "RaftTransport");
    assert!(report
        .capability_evidence
        .iter()
        .any(|item| item.capability == "leader_write_authority"));
    assert!(report
        .parity
        .byteraft_reference_policy
        .feature_reference
        .contains("ByteRaft"));
}
