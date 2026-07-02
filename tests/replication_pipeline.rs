use rustraft::{
    RaftCluster, RaftReplicationPipeline, RustRaftAppendEntriesResponse, RustRaftLogEntry,
    RustRaftLogId, RustRaftPeer, RustRaftPipelineLimits, RustRaftReplicaRole,
};

fn entry(index: u64, payload: &[u8]) -> RustRaftLogEntry {
    RustRaftLogEntry {
        log_id: RustRaftLogId { term: 1, index },
        payload: payload.to_vec(),
    }
}

fn peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 14_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 15_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn small_limits() -> RustRaftPipelineLimits {
    RustRaftPipelineLimits {
        max_inflights_replicate: 1,
        max_memory_replicate_log_bytes: 16,
        max_inflights_apply_task: 2,
        max_apply_batch_bytes: 8,
        enable_reorder_queue: true,
        reorder_window_size: 2,
        reorder_timeout_us: 10,
    }
}

#[test]
fn replication_pipeline_batches_retries_backoff_and_lag() {
    let mut pipeline = RaftReplicationPipeline::new(
        2,
        1,
        RustRaftPipelineLimits {
            max_inflights_replicate: 8,
            max_memory_replicate_log_bytes: 1024,
            max_inflights_apply_task: 2,
            max_apply_batch_bytes: 256,
            enable_reorder_queue: true,
            reorder_window_size: 8,
            reorder_timeout_us: 10,
        },
    );

    pipeline.queue_append(entry(1, b"one")).expect("queue one");
    pipeline.queue_append(entry(2, b"two")).expect("queue two");
    pipeline
        .queue_append(entry(3, b"three"))
        .expect("queue three");

    let flushed = pipeline.flush_append_batch(3, 1024);
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].entry_count, 3);
    assert_eq!(flushed[0].first_log_id.index, 1);
    assert_eq!(flushed[0].last_log_id.index, 3);
    assert_eq!(pipeline.status().append_batches, 1);
    assert_eq!(pipeline.status().max_append_batch_entries, 3);

    assert!(pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: false,
            match_index: 0,
            rejection_hint: Some(0),
        })
        .is_err());
    assert_eq!(pipeline.status().retry_attempts, 1);
    assert!(pipeline.status().backoff_ms > 0);
    assert!(!pipeline.record_retry_backoff_tick(1));
    let remaining = pipeline.status().next_retry_after_ms;
    assert!(pipeline.record_retry_backoff_tick(remaining));

    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 3,
            rejection_hint: None,
        })
        .expect("ack batch");
    assert_eq!(pipeline.status().retry_attempts, 0);
    assert_eq!(pipeline.status().inflight_entries, 0);
    assert_eq!(pipeline.update_follower_lag(5), 2);
}

#[test]
fn replication_pipeline_enforces_windows_and_memory_backpressure() {
    let mut pipeline = RaftReplicationPipeline::new(2, 1, small_limits());

    pipeline
        .queue_append(entry(1, b"12345678"))
        .expect("queue first");
    let flushed = pipeline.flush_append_window();
    assert_eq!(flushed.len(), 1);
    assert_eq!(pipeline.status().inflight_entries, 1);
    assert_eq!(pipeline.status().inflight_bytes, 8);

    pipeline
        .queue_append(entry(2, b"abcd"))
        .expect("queue second");
    assert!(pipeline.queue_append(entry(3, b"efgh")).is_err());
    assert_eq!(pipeline.status().append_queue_depth, 1);
    assert_eq!(pipeline.status().apply_backpressure_rejections, 1);

    pipeline
        .handle_append_response(&RustRaftAppendEntriesResponse {
            term: 1,
            success: true,
            match_index: 1,
            rejection_hint: None,
        })
        .expect("ack first");
    assert_eq!(pipeline.status().inflight_entries, 0);
    assert_eq!(pipeline.flush_append_window().len(), 1);

    let mut memory_limited = RaftReplicationPipeline::new(
        3,
        1,
        RustRaftPipelineLimits {
            max_inflights_replicate: 8,
            max_memory_replicate_log_bytes: 10,
            max_inflights_apply_task: 2,
            max_apply_batch_bytes: 8,
            enable_reorder_queue: true,
            reorder_window_size: 2,
            reorder_timeout_us: 10,
        },
    );
    memory_limited
        .queue_append(entry(1, b"12345678"))
        .expect("queue memory baseline");
    assert!(memory_limited.queue_append(entry(2, b"abcd")).is_err());
    assert_eq!(memory_limited.status().memory_backpressure_rejections, 1);
}

#[test]
fn replication_pipeline_drains_and_expires_reorder_queue() {
    let mut pipeline = RaftReplicationPipeline::new(2, 1, small_limits());

    pipeline
        .receive_out_of_order(entry(3, b"three"))
        .expect("queue out of order");
    assert_eq!(pipeline.status().reorder_queue_depth, 1);

    pipeline
        .receive_out_of_order(entry(1, b"one"))
        .expect("accept next index");
    assert_eq!(pipeline.status().match_index, 1);
    assert_eq!(pipeline.status().reorder_queue_depth, 1);

    pipeline
        .receive_out_of_order(entry(2, b"two"))
        .expect("drain through queued three");
    assert_eq!(pipeline.status().match_index, 3);
    assert_eq!(pipeline.status().reorder_queue_depth, 0);

    pipeline
        .receive_out_of_order(entry(5, b"five"))
        .expect("queue gap");
    assert_eq!(pipeline.expire_reorder_queue(), 1);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 1);
    assert_eq!(pipeline.status().reorder_dropped_packages, 1);
}

#[test]
fn replication_pipeline_tracks_snapshot_sender_and_receiver_state() {
    let mut sender = RaftReplicationPipeline::new(2, 10, RustRaftPipelineLimits::default());
    sender
        .begin_snapshot_send("snap-20", 20, 2)
        .expect("begin send");
    assert!(sender.status().snapshot_sending);
    assert_eq!(sender.status().snapshot_send_attempts, 1);
    sender
        .record_snapshot_chunk_sent(128)
        .expect("record sent bytes");
    sender
        .acknowledge_snapshot_chunk()
        .expect("ack first chunk");
    assert_eq!(sender.status().snapshot_install_progress_per_mille, 500);
    sender
        .acknowledge_snapshot_chunk()
        .expect("ack final chunk");
    assert!(!sender.status().snapshot_sending);
    assert_eq!(sender.status().snapshot_installed_index, 20);

    let mut receiver = RaftReplicationPipeline::new(1, 1, RustRaftPipelineLimits::default());
    receiver
        .begin_snapshot_install("snap-40", 40, 2)
        .expect("begin install");
    assert!(receiver.status().snapshot_installing);
    receiver
        .receive_snapshot_chunk(64, false)
        .expect("receive first");
    receiver
        .receive_snapshot_chunk(64, true)
        .expect("receive done");
    assert!(!receiver.status().snapshot_installing);
    assert_eq!(receiver.status().snapshot_installed_index, 40);
}

#[test]
fn raft_cluster_updates_live_peer_pipelines_during_replication() {
    let mut cluster =
        RaftCluster::new(88, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"x".to_vec()).expect("propose");

    let peer_two = cluster.peer_pipeline_status(2).expect("pipeline");
    assert_eq!(peer_two.append_requests, 1);
    assert_eq!(peer_two.append_accepted, 1);
    assert_eq!(peer_two.match_index, 1);
    assert_eq!(peer_two.next_index, 2);

    cluster
        .receive_out_of_order_append_for(2, entry(3, b"future"))
        .expect("track out of order");
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("pipeline")
            .reorder_queue_depth,
        1
    );

    cluster
        .begin_snapshot_send_to(2, "snap-5", 5, 1)
        .expect("begin snapshot send");
    cluster
        .record_snapshot_chunk_sent_to(2, 32)
        .expect("sent chunk");
    cluster.acknowledge_snapshot_chunk_to(2).expect("ack chunk");
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("pipeline")
            .snapshot_installed_index,
        5
    );
}

#[test]
fn raft_cluster_runs_learner_catchup_and_witness_quorum_accounting() {
    let mut cluster =
        RaftCluster::new(99, Default::default(), vec![peer(1), peer(2), peer(3)]).expect("cluster");
    cluster.start().expect("start");
    cluster.propose(b"a".to_vec()).expect("first write");
    cluster.propose(b"b".to_vec()).expect("second write");

    let mut learner = peer(4);
    learner.role = RustRaftReplicaRole::Learner;
    cluster.add_learner(learner).expect("add learner");
    let catchup = cluster.learner_catch_up_loop(4).expect("catch up");
    assert!(catchup.caught_up);
    assert_eq!(
        catchup.learner_match_index_after,
        catchup.leader_commit_index
    );
    assert!(
        cluster
            .catchup_report(4)
            .expect("catchup report")
            .promotable
    );
    let learner_pipeline = cluster.peer_pipeline_status(4).expect("learner pipeline");
    assert_eq!(learner_pipeline.learner_catchup_rounds, 1);
    assert!(learner_pipeline.learner_caught_up);
    assert_eq!(learner_pipeline.follower_lag, 0);

    let mut witness = peer(5);
    witness.role = RustRaftReplicaRole::Witness;
    cluster.add_witness(witness).expect("add witness");
    let quorum = cluster.witness_quorum_report([1, 2, 5]);
    assert_eq!(quorum.required, 3);
    assert_eq!(quorum.acknowledged, 3);
    assert!(quorum.reached);
    assert_eq!(quorum.witnesses, vec![5]);
    let witness_pipeline = cluster.peer_pipeline_status(5).expect("witness pipeline");
    assert_eq!(witness_pipeline.witness_quorum_required, 3);
    assert_eq!(witness_pipeline.witness_quorum_acked, 3);
    assert!(witness_pipeline.witness_quorum_reached);
}
