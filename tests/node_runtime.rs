use rustraft::{
    RaftNodeRuntime, RaftNodeRuntimeState, RustRaftConfig, RustRaftNodeOptions, RustRaftPeer,
    RustRaftReplicaRole,
};

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn node_options() -> RustRaftNodeOptions {
    RustRaftNodeOptions {
        group_id: 77,
        node_id: 1,
        raft_addr: "127.0.0.1:12001".to_string(),
        snapshot_addr: "127.0.0.1:13001".to_string(),
        wal_dir: "/tmp/rustraft-node-runtime-wal".to_string(),
        snapshot_dir: "/tmp/rustraft-node-runtime-snapshot".to_string(),
        role: RustRaftReplicaRole::Voter,
        config: RustRaftConfig::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

#[test]
fn node_runtime_lifecycle_drives_background_cluster() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Created);

    runtime.start().expect("start runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Running);
    let status = runtime.status().expect("status");
    assert!(status.worker_running);
    assert_eq!(status.node_id, 1);
    assert_eq!(status.group_id, 77);
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(1)
    );

    let log_id = runtime
        .propose(b"write through worker".to_vec())
        .expect("propose");
    assert_eq!(log_id.index, 1);
    let read = runtime.read_index(1).expect("read index");
    assert!(read.safe);
    assert_eq!(read.read_index, 1);

    runtime.stop().expect("stop runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Stopped);
    assert!(runtime.propose(b"stopped".to_vec()).is_err());

    runtime.restart().expect("restart runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Running);
    assert_eq!(runtime.restart_count(), 1);
    assert_eq!(runtime.propose(b"after restart".to_vec()).unwrap().index, 2);

    runtime.shutdown().expect("shutdown runtime");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Shutdown);
    assert!(runtime.read_index(1).is_err());
}

#[test]
fn node_runtime_supports_transfer_and_campaign_lifecycle_commands() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    runtime.transfer_leader(2).expect("transfer leader");
    let status = runtime.status().expect("status");
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(2)
    );

    runtime.campaign(true).expect("campaign local node");
    let status = runtime.status().expect("status");
    assert_eq!(
        status.cluster_status.expect("cluster status").leader_id,
        Some(1)
    );
}

#[test]
fn node_runtime_shutdown_is_idempotent() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.shutdown().expect("shutdown runtime");
    runtime.shutdown().expect("second shutdown is ok");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Shutdown);
}
