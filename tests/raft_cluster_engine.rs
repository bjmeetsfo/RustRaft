use rustraft::{
    RaftCluster, RaftConfig, RaftConfigError, RustRaftAppendEntriesRequest, RustRaftConfig,
    RustRaftConsensus, RustRaftError, RustRaftLogEntry, RustRaftLogId, RustRaftPeer,
    RustRaftReadIndexRequest, RustRaftReplicaRole, RustRaftRole,
};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn three_node_cluster() -> RaftCluster {
    RaftCluster::new(
        7,
        RaftConfig::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("valid cluster")
}

#[test]
fn raft_config_validates_timing_and_capacity() {
    let mut config = RustRaftConfig::default();
    config.heartbeat_interval_ms = config.election_timeout_ms;

    assert_eq!(
        config.validate(),
        Err(RaftConfigError::HeartbeatNotLessThanElection {
            heartbeat_interval_ms: 1_000,
            election_timeout_ms: 1_000,
        })
    );

    config = RustRaftConfig::default();
    config.max_payload_bytes = 0;
    assert_eq!(config.validate(), Err(RaftConfigError::ZeroMaxPayloadBytes));
}

#[test]
fn cluster_start_campaigns_and_tracks_leader_term() {
    let mut cluster = three_node_cluster();

    cluster.start().expect("cluster starts");
    assert_eq!(cluster.leader_id(), Some(1));

    let leader_status = cluster.status(1).expect("leader status");
    assert_eq!(leader_status.role, RustRaftRole::Leader);
    assert_eq!(leader_status.term, 1);

    cluster.campaign(2, false).expect("campaign to node 2");
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(cluster.status(2).expect("new leader status").term, 2);
}

#[test]
fn propose_replicates_to_quorum_and_advances_commit_and_apply() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let log_id = cluster.propose(b"set a=1".to_vec()).expect("propose");
    assert_eq!(log_id, RustRaftLogId { term: 1, index: 1 });

    for node_id in [1, 2, 3] {
        let status = cluster.status(node_id).expect("node status");
        assert_eq!(status.commit_index, 1);
        assert_eq!(status.applied_index, 1);
        assert_eq!(status.last_log_index, 1);
    }
}

#[test]
fn append_entries_updates_follower_commit_and_rejects_missing_prev_log() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(RustRaftLogId { term: 1, index: 8 }),
                entries: vec![],
                leader_commit: 8,
            },
        )
        .expect("append response");
    assert!(!response.success);

    let response = cluster
        .append_entries_to(
            2,
            RustRaftAppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![RustRaftLogEntry {
                    log_id: RustRaftLogId { term: 1, index: 1 },
                    payload: b"x".to_vec(),
                }],
                leader_commit: 1,
            },
        )
        .expect("append response");
    assert!(response.success);
    assert_eq!(cluster.status(2).expect("node 2 status").applied_index, 1);
}

#[test]
fn read_index_and_lease_read_follow_leader_lease_and_apply_floor() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");

    let lease = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 1,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(lease.safe);
    assert!(lease.lease_read);

    cluster.set_leader_lease_valid(false);
    let read_index = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 1,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(read_index.safe);
    assert!(!read_index.lease_read);

    let unsafe_read = cluster
        .read_index(RustRaftReadIndexRequest {
            group_id: 7,
            requester_id: 1,
            min_commit_index: 2,
            allow_lease_read: true,
        })
        .expect("read index");
    assert!(!unsafe_read.safe);
}

#[test]
fn leader_transfer_requires_a_caught_up_voter() {
    let mut cluster = three_node_cluster();
    cluster.start().expect("cluster starts");
    cluster.propose(b"set a=1".to_vec()).expect("propose");

    cluster.transfer_leader(2).expect("transfer leader");
    assert_eq!(cluster.leader_id(), Some(2));
    assert_eq!(
        cluster.status(2).expect("node 2 status").role,
        RustRaftRole::Leader
    );

    assert_eq!(
        cluster.transfer_leader(99),
        Err(RustRaftError::NodeNotFound(99))
    );
}

#[test]
fn raft_cluster_implements_consensus_trait_surface() {
    let mut cluster = three_node_cluster();
    RustRaftConsensus::start(&mut cluster).expect("trait start");
    let log_id = RustRaftConsensus::propose(&mut cluster, b"x".to_vec(), Default::default())
        .expect("trait propose");
    assert_eq!(log_id.index, 1);

    let read = RustRaftConsensus::read_index(&cluster, 1).expect("trait read index");
    assert!(read.safe);
    assert_eq!(read.read_index, 1);
}
