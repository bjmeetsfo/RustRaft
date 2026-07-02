use rustraft::{
    rustraft_validate_read_index_response, rustraft_validate_tcp_transport_request,
    rustraft_validate_vote_request, AppendEntriesRequest, AppendEntriesResponse,
    AuthenticatedRaftTransport, ClusterRaftTransport, InMemoryRaftTransport,
    InstallSnapshotRequest, InstallSnapshotResponse, RaftCluster, RaftError, RaftTransport,
    ReadIndexRequest, ReadIndexResponse, RustRaftAppendEntriesRequest,
    RustRaftInstallSnapshotRequest, RustRaftLogEntry, RustRaftLogId, RustRaftPeer,
    RustRaftReplicaRole, RustRaftSnapshotChunk, RustRaftSnapshotMeta, RustRaftTransport,
    StaticRaftAuthToken, TcpRaftTransport, TcpRaftTransportRequest, TcpRaftTransportServer,
    VoteRequest, VoteResponse,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn peer(node_id: u64, role: RustRaftReplicaRole) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 7_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 8_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[derive(Debug, Clone)]
struct EchoTransport;

impl RustRaftTransport for EchoTransport {
    fn append_entries(
        &self,
        _target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        Ok(AppendEntriesResponse {
            term: request.term,
            success: true,
            match_index: request.leader_commit,
            rejection_hint: None,
        })
    }

    fn vote(&self, _target: u64, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        Ok(VoteResponse {
            term: request.term,
            vote_granted: true,
            reason: "granted".to_string(),
        })
    }

    fn install_snapshot(
        &self,
        _target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        Ok(InstallSnapshotResponse {
            term: request.term,
            accepted: true,
            next_offset: request.chunk.offset + request.chunk.data.len() as u64,
            reason: "accepted".to_string(),
        })
    }

    fn read_index(
        &self,
        _target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        Ok(ReadIndexResponse {
            safe: true,
            read_index: request.min_commit_index,
            lease_read: request.allow_lease_read,
            reason: "read_index".to_string(),
        })
    }
}

fn assert_raft_transport<T: RaftTransport>(_transport: &T) {}

#[test]
fn transport_aliases_cover_all_rpc_messages() {
    let append: AppendEntriesRequest = AppendEntriesRequest {
        group_id: 3,
        term: 2,
        leader_id: 1,
        prev_log_id: Some(RustRaftLogId { term: 2, index: 4 }),
        entries: Vec::new(),
        leader_commit: 4,
    };
    let vote: VoteRequest = VoteRequest {
        group_id: 3,
        term: 2,
        candidate_id: 1,
        last_log_id: append.prev_log_id.clone(),
        pre_vote: true,
    };
    let snapshot: InstallSnapshotRequest = InstallSnapshotRequest {
        group_id: 3,
        term: 2,
        leader_id: 1,
        chunk: RustRaftSnapshotChunk {
            meta: RustRaftSnapshotMeta {
                snapshot_id: "snap".to_string(),
                last_log_id: RustRaftLogId { term: 2, index: 4 },
                membership: vec![1, 2, 3],
            },
            offset: 0,
            data: b"snapshot".to_vec(),
            done: true,
        },
    };
    let read: ReadIndexRequest = ReadIndexRequest {
        group_id: 3,
        requester_id: 1,
        min_commit_index: 4,
        allow_lease_read: true,
    };

    assert_eq!(vote.candidate_id, append.leader_id);
    assert_eq!(snapshot.chunk.data, b"snapshot");
    assert!(read.allow_lease_read);
}

#[test]
fn authenticated_transport_wrapper_accepts_and_rejects_tokens() {
    let transport =
        AuthenticatedRaftTransport::new(EchoTransport, StaticRaftAuthToken::new("secret"));
    assert_raft_transport(&transport);

    let request = transport.wrap_request(
        2,
        ReadIndexRequest {
            group_id: 3,
            requester_id: 1,
            min_commit_index: 7,
            allow_lease_read: true,
        },
    );
    let response = transport
        .read_index_authenticated(2, request)
        .expect("authenticated read");
    assert_eq!(response.read_index, 7);
    assert!(response.lease_read);

    let rejected = transport.read_index_authenticated(
        2,
        rustraft::AuthenticatedRaftRpc {
            auth: "wrong".to_string(),
            message: ReadIndexRequest {
                group_id: 3,
                requester_id: 1,
                min_commit_index: 7,
                allow_lease_read: false,
            },
        },
    );
    assert!(matches!(rejected, Err(RaftError::Transport(_))));
}

#[test]
fn transport_validation_reports_bad_requests_and_responses() {
    let bad_vote = VoteRequest {
        group_id: 0,
        term: 1,
        candidate_id: 0,
        last_log_id: Some(RustRaftLogId { term: 1, index: 0 }),
        pre_vote: true,
    };
    let vote_report = rustraft_validate_vote_request(&bad_vote);
    assert!(!vote_report.valid);
    assert!(vote_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("group_id")));
    assert!(vote_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("candidate_id")));

    let bad_read_response = ReadIndexResponse {
        safe: false,
        read_index: 5,
        lease_read: true,
        reason: "".to_string(),
    };
    let read_report = rustraft_validate_read_index_response(&bad_read_response);
    assert!(!read_report.valid);
    assert!(read_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("lease_read")));

    let bad_tcp = TcpRaftTransportRequest::Vote {
        target: 0,
        request: bad_vote,
    };
    let tcp_report = rustraft_validate_tcp_transport_request(&bad_tcp);
    assert!(!tcp_report.valid);
    assert!(tcp_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("target")));
}

#[test]
fn in_memory_transport_forwards_and_validates_all_rpc_messages() {
    let transport = InMemoryRaftTransport::new();
    transport.register(2, EchoTransport).expect("register peer");
    assert_raft_transport(&transport);

    let append = transport
        .append_entries(
            2,
            AppendEntriesRequest {
                group_id: 3,
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
        .expect("append through memory transport");
    assert!(append.success);

    let vote = transport
        .vote(
            2,
            VoteRequest {
                group_id: 3,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: true,
            },
        )
        .expect("vote through memory transport");
    assert!(vote.vote_granted);

    let snapshot = transport
        .install_snapshot(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 2,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "memory-snap".to_string(),
                        last_log_id: RustRaftLogId { term: 2, index: 4 },
                        membership: vec![1, 2, 3],
                    },
                    offset: 0,
                    data: b"snapshot".to_vec(),
                    done: true,
                },
            },
        )
        .expect("snapshot through memory transport");
    assert!(snapshot.accepted);

    let read = transport
        .read_index(
            2,
            ReadIndexRequest {
                group_id: 3,
                requester_id: 1,
                min_commit_index: 4,
                allow_lease_read: true,
            },
        )
        .expect("read-index through memory transport");
    assert!(read.safe);
    assert!(read.lease_read);

    let rejected = transport.read_index(
        2,
        ReadIndexRequest {
            group_id: 0,
            requester_id: 1,
            min_commit_index: 4,
            allow_lease_read: false,
        },
    );
    assert!(matches!(rejected, Err(RaftError::InvalidRequest(_))));
}

#[test]
fn cluster_installs_snapshot_from_chunked_snapshot_rpc() {
    let mut cluster = RaftCluster::new(
        3,
        Default::default(),
        vec![
            peer(1, RustRaftReplicaRole::Voter),
            peer(2, RustRaftReplicaRole::Voter),
            peer(3, RustRaftReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");

    let response = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "snap-9".to_string(),
                        last_log_id: RustRaftLogId { term: 1, index: 9 },
                        membership: vec![1, 2, 3],
                    },
                    offset: 0,
                    data: b"state".to_vec(),
                    done: true,
                },
            },
        )
        .expect("install snapshot rpc");
    assert!(response.accepted);
    assert_eq!(response.reason, "snapshot_installed");
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 9);
}

#[test]
fn tcp_transport_round_trips_append_snapshot_vote_and_read_index() {
    let cluster = Arc::new(Mutex::new(
        RaftCluster::new(
            3,
            Default::default(),
            vec![
                peer(1, RustRaftReplicaRole::Voter),
                peer(2, RustRaftReplicaRole::Voter),
                peer(3, RustRaftReplicaRole::Voter),
            ],
        )
        .expect("cluster"),
    ));
    cluster.lock().expect("lock").start().expect("start");
    let handler = Arc::new(ClusterRaftTransport::new(Arc::clone(&cluster)));
    let mut server =
        TcpRaftTransportServer::start("127.0.0.1:0", handler).expect("start tcp server");

    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = TcpRaftTransport::new(peers);
    let append = transport
        .append_entries(
            2,
            AppendEntriesRequest {
                group_id: 3,
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
        .expect("append over tcp");
    assert!(append.success);
    assert_eq!(append.match_index, 1);

    let vote = transport
        .vote(
            2,
            VoteRequest {
                group_id: 3,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(RustRaftLogId { term: 1, index: 1 }),
                pre_vote: true,
            },
        )
        .expect("vote over tcp");
    assert!(vote.vote_granted);

    let read = transport
        .read_index(
            2,
            ReadIndexRequest {
                group_id: 3,
                requester_id: 2,
                min_commit_index: 1,
                allow_lease_read: true,
            },
        )
        .expect("read over tcp");
    assert!(read.safe);

    let snapshot = transport
        .install_snapshot(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: RustRaftSnapshotChunk {
                    meta: RustRaftSnapshotMeta {
                        snapshot_id: "tcp-snap".to_string(),
                        last_log_id: RustRaftLogId { term: 1, index: 4 },
                        membership: vec![1, 2, 3],
                    },
                    offset: 0,
                    data: b"state".to_vec(),
                    done: true,
                },
            },
        )
        .expect("snapshot over tcp");
    assert!(snapshot.accepted);
    assert_eq!(
        cluster
            .lock()
            .expect("lock")
            .status(2)
            .expect("status")
            .last_snapshot_index,
        4
    );

    server.shutdown().expect("shutdown server");
}
