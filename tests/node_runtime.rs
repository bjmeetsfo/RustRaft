use rustraft::{
    RaftNodeRuntime, RaftNodeRuntimeState, RustRaftConfig, RustRaftNodeOptions, RustRaftPeer,
    RustRaftReplicaRole,
};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn temp_runtime_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-node-runtime-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn node_options() -> RustRaftNodeOptions {
    node_options_in(temp_runtime_dir("default"))
}

fn node_options_in(base_dir: PathBuf) -> RustRaftNodeOptions {
    RustRaftNodeOptions {
        group_id: 77,
        node_id: 1,
        raft_addr: "127.0.0.1:12001".to_string(),
        snapshot_addr: "127.0.0.1:13001".to_string(),
        wal_dir: base_dir.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base_dir.join("snapshot").to_string_lossy().into_owned(),
        role: RustRaftReplicaRole::Voter,
        config: RustRaftConfig::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

fn timer_node_options() -> RustRaftNodeOptions {
    let mut options = node_options_in(temp_runtime_dir("timer"));
    options.config.heartbeat_interval_ms = 10;
    options.config.election_timeout_ms = 50;
    options.config.leader_lease_ms = 20;
    options
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
    assert!(read.lease_read);

    runtime
        .set_leader_lease_valid(false)
        .expect("stale leader lease");
    let read = runtime.read_index(1).expect("read index without lease");
    assert!(read.safe);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "read_index");

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
fn node_runtime_read_index_rejects_without_live_quorum() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.propose(b"write".to_vec()).expect("propose");
    runtime
        .set_node_healthy(2, false)
        .expect("mark node 2 down");
    runtime
        .set_node_healthy(3, false)
        .expect("mark node 3 down");

    let read = runtime.read_index(1).expect("read index");
    assert!(!read.safe);
    assert!(!read.lease_read);
    assert_eq!(read.reason, "no_live_quorum");
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
fn node_runtime_runs_heartbeat_election_timer_loop_and_peer_state_machine() {
    let mut runtime = RaftNodeRuntime::create(timer_node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    std::thread::sleep(Duration::from_millis(80));

    let status = runtime.status().expect("status");
    assert!(status.timer_status.heartbeat_ticks > 0);
    assert!(status.timer_status.election_ticks > 0);
    assert_eq!(status.timer_status.heartbeat_interval_ms, 10);
    assert_eq!(status.timer_status.election_timeout_ms, 50);
    assert_eq!(status.peer_runtime.len(), 3);
    assert!(status
        .peer_runtime
        .iter()
        .any(|peer| peer.node_id == 1 && peer.transfer_leader_target));
    assert!(status.fatal_blocker_report.ready);
}

#[test]
fn node_runtime_executes_prevote_and_reports_blockers_from_runtime_tasks() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");

    let vote = runtime.pre_vote().expect("pre-vote");
    assert!(vote.vote_granted);
    assert_eq!(vote.reason, "pre_vote_granted");

    runtime
        .set_node_healthy(2, false)
        .expect("mark node 2 down");
    runtime
        .set_node_healthy(3, false)
        .expect("mark node 3 down");
    assert!(runtime.transfer_leader(2).is_err());

    let status = runtime.status().expect("status");
    assert!(status.timer_status.pre_vote_executions >= 1);
    assert!(status.timer_status.leader_transfer_executions >= 1);
    assert!(!status.fatal_blocker_report.ready);
    assert!(status
        .fatal_blocker_report
        .blockers
        .iter()
        .any(|blocker| blocker.id.contains("transfer_leader")));
}

#[test]
fn node_runtime_shutdown_is_idempotent() {
    let mut runtime = RaftNodeRuntime::create(node_options()).expect("create runtime");
    runtime.start().expect("start runtime");
    runtime.shutdown().expect("shutdown runtime");
    runtime.shutdown().expect("second shutdown is ok");
    assert_eq!(runtime.state(), RaftNodeRuntimeState::Shutdown);
}

#[test]
fn node_runtime_recovers_committed_index_from_persistent_wal() {
    let base_dir = temp_runtime_dir("wal-recovery");
    let options = node_options_in(base_dir.clone());
    {
        let mut runtime = RaftNodeRuntime::create(options.clone()).expect("create runtime");
        runtime.start().expect("start runtime");
        assert_eq!(runtime.propose(b"one".to_vec()).expect("first").index, 1);
        assert_eq!(runtime.propose(b"two".to_vec()).expect("second").index, 2);
        runtime.shutdown().expect("shutdown");
    }

    let mut recovered = RaftNodeRuntime::create(options).expect("recreate runtime");
    recovered.start().expect("start recovered runtime");
    let read = recovered.read_index(2).expect("read recovered index");
    assert!(read.safe);
    assert_eq!(read.read_index, 2);
    assert_eq!(
        recovered
            .propose(b"three".to_vec())
            .expect("post recovery write")
            .index,
        3
    );
    recovered.shutdown().expect("shutdown recovered");

    let _ = fs::remove_dir_all(base_dir);
}
