use rustraft::{
    rustraft_snapshot_lifecycle_evidence, PersistentRaftSnapshotStore,
    PersistentRaftSnapshotStoreOptions, RaftCluster, RaftSnapshot, RaftSnapshotLifecycle,
    RaftSnapshotLifecycleConfig, RustRaftApplySnapshotFence, RustRaftInstallSnapshotResponse,
    RustRaftLogEntry, RustRaftLogId, RustRaftPeer, RustRaftReplicaRole, RustRaftSnapshotMeta,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_snapshot_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-snapshot-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 16_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 17_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn snapshot(index: u64, payload: &[u8]) -> RaftSnapshot {
    RaftSnapshot {
        group_id: 55,
        meta: RustRaftSnapshotMeta {
            snapshot_id: format!("snap-{index}"),
            last_log_id: RustRaftLogId { term: 2, index },
            membership: vec![1, 2, 3],
        },
        payload: payload.to_vec(),
    }
}

fn tail_entry(index: u64) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term: 2, index },
        payload: format!("tail-{index}").into_bytes(),
    }
}

#[test]
fn snapshot_lifecycle_throttles_retries_and_rolls_back_install() {
    let snap = snapshot(10, b"abcdefghijklmnopqrstuvwxyz");
    let mut lifecycle = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 3,
        max_chunks_per_tick: 4,
        max_bytes_per_tick: 4,
        max_retry_attempts: 2,
    })
    .expect("lifecycle");

    lifecycle.begin_send(&snap, 2, 1).expect("begin send");
    let first = lifecycle.poll_send_requests().expect("first tick");
    assert_eq!(first.len(), 1);
    assert!(lifecycle.status().throttled_ticks > 0);

    lifecycle
        .record_send_response(&RustRaftInstallSnapshotResponse {
            term: 2,
            accepted: false,
            next_offset: 0,
            reason: "retry".to_string(),
        })
        .expect("retry response");
    assert_eq!(lifecycle.status().retry_count, 1);
    let resent = lifecycle.poll_send_requests().expect("retry tick");
    assert_eq!(resent[0].chunk.offset, 0);

    let mut installer = RaftSnapshotLifecycle::new(Default::default()).expect("installer");
    assert!(installer
        .install_request(first[0].clone())
        .expect("partial")
        .is_none());
    assert!(installer.status().installing);
    installer.rollback_install();
    assert!(!installer.status().installing);
    assert_eq!(installer.status().rolled_back, 1);
}

#[test]
fn snapshot_checkpoint_store_saves_loads_and_rechunks() {
    let dir = temp_snapshot_dir("checkpoint");
    let store = PersistentRaftSnapshotStore::open(PersistentRaftSnapshotStoreOptions {
        dir: dir.clone(),
        chunk_size: 4,
    })
    .expect("store");
    let snap = snapshot(12, b"hello snapshot store");
    let path = store.save_checkpoint(&snap).expect("save");
    assert!(path.exists());

    let loaded = store.load_checkpoint("snap-12").expect("load");
    assert_eq!(loaded, snap);
    let chunks = store.checkpoint_chunks("snap-12").expect("chunks");
    assert!(chunks.len() > 1);
    assert_eq!(chunks.first().expect("first").offset, 0);
    assert!(chunks.last().expect("last").done);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn cluster_installs_snapshot_with_lifecycle_then_catches_up_tail_after_compaction() {
    let mut cluster =
        RaftCluster::new(55, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    for index in 1..=6 {
        cluster
            .propose(format!("write-{index}").into_bytes())
            .expect("propose");
    }
    let removed = cluster.compact_logs_through(4);
    assert!(removed > 0);

    let snap = snapshot(4, b"checkpoint-through-four");
    let mut sender = RaftSnapshotLifecycle::new(RaftSnapshotLifecycleConfig {
        chunk_size: 5,
        max_chunks_per_tick: 2,
        max_bytes_per_tick: 10,
        max_retry_attempts: 3,
    })
    .expect("sender");
    let mut receiver = RaftSnapshotLifecycle::new(Default::default()).expect("receiver");
    sender.begin_send(&snap, 2, 1).expect("begin send");

    while sender.status().sending {
        let requests = sender.poll_send_requests().expect("poll");
        for request in requests {
            let response = cluster
                .install_snapshot_lifecycle_request_to(3, &mut receiver, request)
                .expect("install lifecycle request");
            sender
                .record_send_response(&response)
                .expect("record response");
        }
    }
    assert_eq!(cluster.status(3).expect("status").last_snapshot_index, 4);

    cluster
        .install_snapshot_with_tail_to(
            3,
            snap,
            RustRaftApplySnapshotFence {
                applied_index: 4,
                commit_index: 4,
                installed_snapshot_index: 4,
                first_retained_log_index: 5,
            },
            vec![tail_entry(5), tail_entry(6)],
        )
        .expect("tail catch-up");
    assert_eq!(cluster.status(3).expect("status").last_log_index, 6);

    let peer_three = cluster.peer_pipeline_status(3).expect("pipeline");
    let evidence = rustraft_snapshot_lifecycle_evidence(&[peer_three], 1_000, 1);
    assert!(evidence.sender_lifecycle_present || evidence.downloader_lifecycle_present);
    assert!(evidence.install_progress_present);
    assert!(evidence.rejoin_after_compacted_log_present);
}
