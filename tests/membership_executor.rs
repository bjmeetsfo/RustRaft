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
