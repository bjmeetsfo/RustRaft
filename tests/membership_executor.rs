use rustraft::{
    RaftCluster, RaftMembershipExecutor, RaftMembershipOperation, RustRaftApplySnapshotFence,
    RustRaftLogId, RustRaftPeer, RustRaftReplicaRole, RustRaftSnapshotMeta,
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 18_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 19_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[test]
fn membership_executor_runs_full_runtime_workflow() {
    let mut cluster = RaftCluster::new(
        66,
        Default::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("write");

    let mut executor = RaftMembershipExecutor::new();
    executor
        .execute(
            &mut cluster,
            RaftMembershipOperation::AddLearner(peer(4, RustRaftReplicaRole::Voter)),
        )
        .expect("add learner");
    assert!(cluster.membership().learners.contains(&4));

    let rejected = executor
        .execute(&mut cluster, RaftMembershipOperation::Promote(4))
        .expect_err("cannot promote lagging learner");
    assert!(rejected.to_string().contains("learner_lagging"));

    let snapshot = rustraft::RaftSnapshot {
        group_id: 66,
        meta: RustRaftSnapshotMeta {
            snapshot_id: "learner-catchup".to_string(),
            last_log_id: RustRaftLogId { term: 1, index: 1 },
            membership: vec![1, 2, 3, 4],
        },
        payload: b"snapshot".to_vec(),
    };
    cluster
        .install_snapshot_to(
            4,
            snapshot,
            RustRaftApplySnapshotFence {
                applied_index: 1,
                commit_index: 1,
                installed_snapshot_index: 1,
                first_retained_log_index: 2,
            },
        )
        .expect("catch up learner");

    let reports = executor
        .execute_all(
            &mut cluster,
            vec![
                RaftMembershipOperation::Promote(4),
                RaftMembershipOperation::AddWitness(peer(5, RustRaftReplicaRole::Voter)),
                RaftMembershipOperation::TransferLeader(4),
                RaftMembershipOperation::Remove(2),
            ],
        )
        .expect("execute workflow");

    assert_eq!(reports.len(), 4);
    assert!(reports.iter().all(|report| report.success));
    assert_eq!(cluster.leader_id(), Some(4));
    let membership = cluster.membership();
    assert!(membership.voters.contains(&4));
    assert!(membership.witnesses.contains(&5));
    assert!(!membership.voters.contains(&2));
    assert_eq!(executor.reports().len(), 6);
    assert!(!executor.reports()[1].success);
}

#[test]
fn membership_executor_validates_reports_joint_changes_and_rolls_back() {
    let mut cluster = RaftCluster::new(
        67,
        Default::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("write");

    let mut executor = RaftMembershipExecutor::new();
    let add_voter = executor
        .execute(
            &mut cluster,
            RaftMembershipOperation::AddVoter(peer(4, RustRaftReplicaRole::Learner)),
        )
        .expect("add voter");
    assert!(add_voter.validation_passed);
    assert!(add_voter.success);
    assert!(add_voter.joint_consensus.is_some());
    assert!(cluster.membership().voters.contains(&4));

    let remove_leader = executor
        .execute(&mut cluster, RaftMembershipOperation::Remove(1))
        .expect_err("leader removal should be blocked");
    assert!(remove_leader
        .to_string()
        .contains("cannot_remove_current_leader_without_transfer"));
    let failed_report = executor.reports().last().expect("failed report");
    assert!(!failed_report.validation_passed);
    assert!(!failed_report.success);
    assert!(failed_report
        .blockers
        .contains(&"cannot_remove_current_leader_without_transfer".to_string()));

    let voters_before = cluster.membership().voters;
    let rollback = executor.execute_all_with_rollback(
        &mut cluster,
        vec![
            RaftMembershipOperation::TransferLeader(4),
            RaftMembershipOperation::Remove(1),
            RaftMembershipOperation::Remove(4),
        ],
    );
    assert!(rollback.is_err());
    assert_eq!(cluster.membership().voters, voters_before);
    assert_eq!(cluster.leader_id(), Some(1));
    let rollback_report = executor.reports().last().expect("rollback report");
    assert!(rollback_report.rolled_back);
    assert!(rollback_report.reason.contains("rolled_back"));
}
