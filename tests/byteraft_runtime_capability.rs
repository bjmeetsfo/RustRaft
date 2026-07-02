use rustraft::{
    rustraft_byteraft_runtime_capability_report, RustRaftDataNodeProcessRolloutReport,
    RustRaftMembershipScope, RustRaftMembershipTransitionEvidence,
    RustRaftMembershipTransitionKind, RustRaftMetaProcessRolloutReport, RustRaftPipelineEvidence,
    RustRaftProcessNodeEvidence, RustRaftProcessOperationalSemanticsEvidence,
    RustRaftProductionReadinessInput, RustRaftReadinessSnapshot, RustRaftSnapshotLifecycleEvidence,
    RustRaftWalLifecycleEvidence,
};

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

fn ready_semantics() -> RustRaftProcessOperationalSemanticsEvidence {
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

fn process_nodes() -> Vec<RustRaftProcessNodeEvidence> {
    vec![
        RustRaftProcessNodeEvidence {
            node_id: 1,
            addr: "127.0.0.1:21001".to_string(),
            wal_dir: "/tmp/rustraft/capability/node-1/wal".to_string(),
            snapshot_dir: "/tmp/rustraft/capability/node-1/snapshots".to_string(),
            commit_index: 64,
            applied_index: 64,
            snapshot_id: Some("snap-60".to_string()),
            restarted: true,
            log_store_validated: true,
        },
        RustRaftProcessNodeEvidence {
            node_id: 2,
            addr: "127.0.0.1:21002".to_string(),
            wal_dir: "/tmp/rustraft/capability/node-2/wal".to_string(),
            snapshot_dir: "/tmp/rustraft/capability/node-2/snapshots".to_string(),
            commit_index: 64,
            applied_index: 64,
            snapshot_id: Some("snap-60".to_string()),
            restarted: true,
            log_store_validated: true,
        },
    ]
}

fn ready_data_rollout() -> RustRaftDataNodeProcessRolloutReport {
    RustRaftDataNodeProcessRolloutReport {
        shard_id: 11,
        voters: vec![1, 2, 3],
        learners: vec![4],
        nodes: process_nodes(),
        spawned_process_count: 2,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 20,
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
        operational_semantics: ready_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

fn ready_meta_rollout() -> RustRaftMetaProcessRolloutReport {
    RustRaftMetaProcessRolloutReport {
        voters: vec![1, 2, 3],
        learners: vec![4],
        nodes: process_nodes(),
        spawned_process_count: 2,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 24,
        read_index_responses_observed: 9,
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
        operational_semantics: ready_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

fn transition(
    scope: RustRaftMembershipScope,
    transition: RustRaftMembershipTransitionKind,
) -> RustRaftMembershipTransitionEvidence {
    let (before_voters, after_voters, before_learners, after_learners, added, removed) =
        match transition {
            RustRaftMembershipTransitionKind::Failover => (
                vec![1, 2, 3],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![1],
            ),
            RustRaftMembershipTransitionKind::ScaleUp => (
                vec![1, 2, 3],
                vec![1, 2, 3, 4],
                vec![4],
                Vec::new(),
                vec![4],
                Vec::new(),
            ),
            RustRaftMembershipTransitionKind::ScaleDown => (
                vec![1, 2, 3, 4],
                vec![1, 2, 3],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![4],
            ),
        };
    RustRaftMembershipTransitionEvidence {
        scope,
        transition,
        before_voters,
        after_voters,
        before_learners,
        after_learners,
        leader_before: Some(1),
        leader_after: Some(2),
        failed_or_removed_nodes: removed,
        added_nodes: added,
        caught_up_nodes: vec![1, 2, 3],
        commit_index_before: 90,
        commit_index_after: 96,
        applied_index_after: 96,
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
    }
}

fn transitions() -> Vec<RustRaftMembershipTransitionEvidence> {
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
        .map(move |kind| transition(scope, kind))
    })
    .collect()
}

fn ready_input() -> RustRaftProductionReadinessInput {
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
        data_node_rollout: Some(ready_data_rollout()),
        metaserver_rollout: Some(ready_meta_rollout()),
        membership_transitions: transitions(),
    }
}

#[test]
fn byteraft_runtime_capability_report_accepts_complete_evidence() {
    let report = rustraft_byteraft_runtime_capability_report(&ready_input());
    assert!(report.ready, "{report:#?}");
    assert!(report.missing.is_empty());
    assert!(report.blockers.is_empty());
    assert!(report
        .satisfied
        .contains(&"wal_segment_lifecycle".to_string()));
    assert!(report
        .satisfied
        .contains(&"read_index_and_lease_safety".to_string()));
}

#[test]
fn byteraft_runtime_capability_report_fails_closed_on_missing_wal_lifecycle() {
    let mut input = ready_input();
    input.wal_lifecycle = Some(RustRaftWalLifecycleEvidence {
        segment_lifecycle_present: true,
        retained_range_present: true,
        sequence_range_present: true,
        log_index_range_present: true,
        compaction_observed: true,
        slow_fsync_backpressure_observed: false,
    });

    let report = rustraft_byteraft_runtime_capability_report(&input);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"wal_segment_lifecycle".to_string()));
    assert!(report.blockers.iter().any(|blocker| {
        blocker == "wal_segment_lifecycle:missing:wal.slow_fsync_backpressure_observed"
    }));
}

#[test]
fn byteraft_runtime_capability_report_names_process_path_missing_fields() {
    let mut input = ready_input();
    input
        .data_node_rollout
        .as_mut()
        .unwrap()
        .observed_process_requests = 0;
    input
        .metaserver_rollout
        .as_mut()
        .unwrap()
        .per_node_log_store_inspection_count = 0;

    let report = rustraft_byteraft_runtime_capability_report(&input);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"process_path_rollout_evidence".to_string()));
    assert!(report.blockers.iter().any(|blocker| {
        blocker == "process_path_rollout_evidence:missing:data_node.observed_process_requests"
    }));
    assert!(report.blockers.iter().any(|blocker| {
        blocker == "process_path_rollout_evidence:missing:metaserver.per_node_log_store_inspection"
    }));
}

#[test]
fn byteraft_runtime_capability_report_requires_read_safety_on_both_planes() {
    let mut input = ready_input();
    input
        .metaserver_rollout
        .as_mut()
        .unwrap()
        .operational_semantics
        .minority_partition_read_rejection_observed = false;

    let report = rustraft_byteraft_runtime_capability_report(&input);
    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"read_index_and_lease_safety".to_string()));
    assert!(report.blockers.iter().any(|blocker| {
        blocker
            == "read_index_and_lease_safety:missing:semantics.minority_partition_read_rejection_observed"
    }));
}
