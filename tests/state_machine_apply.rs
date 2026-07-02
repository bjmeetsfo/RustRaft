use rustraft::{
    rustraft_apply_entry, EntryPayload, RaftApply, RaftApplyRequest, RaftApplyResponse,
    RaftLogEntry, RaftStateMachine, RustRaftApplyRequest, RustRaftApplyResponse, RustRaftLogId,
    RustRaftSnapshotChunk, RustRaftSnapshotMeta, RustRaftStateMachine,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct DataShardPayload {
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaPayload {
    assignment: String,
}

#[derive(Default)]
struct DataShardStateMachine {
    applied: Vec<(String, Vec<u8>)>,
}

impl RaftApply<String, DataShardPayload> for DataShardStateMachine {
    type Response = Vec<u8>;

    fn apply(
        &mut self,
        request: RaftApplyRequest<String, DataShardPayload>,
    ) -> Result<RaftApplyResponse<Self::Response>, rustraft::RaftError> {
        assert_eq!(request.group_id, "tenant-a/shard-7");
        self.applied
            .push((request.payload.key, request.payload.value.clone()));
        Ok(RaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload.value,
        })
    }
}

impl RaftStateMachine<String, DataShardPayload> for DataShardStateMachine {
    type Snapshot = Vec<(String, Vec<u8>)>;

    fn snapshot(&self, _group_id: String) -> Result<Self::Snapshot, rustraft::RaftError> {
        Ok(self.applied.clone())
    }

    fn install_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), rustraft::RaftError> {
        self.applied = snapshot;
        Ok(())
    }
}

#[derive(Default)]
struct MetaStateMachine {
    assignments: Vec<String>,
}

impl RaftApply<u64, MetaPayload> for MetaStateMachine {
    type Response = String;

    fn apply(
        &mut self,
        request: RaftApplyRequest<u64, MetaPayload>,
    ) -> Result<RaftApplyResponse<Self::Response>, rustraft::RaftError> {
        assert_eq!(request.group_id, 42);
        self.assignments.push(request.payload.assignment.clone());
        Ok(RaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload.assignment,
        })
    }
}

#[derive(Default)]
struct OpaqueBytesStateMachine {
    applied: Vec<EntryPayload>,
}

impl RustRaftStateMachine for OpaqueBytesStateMachine {
    fn apply(
        &mut self,
        request: RustRaftApplyRequest,
    ) -> Result<RustRaftApplyResponse, rustraft::RustRaftError> {
        self.applied.push(request.payload.clone());
        Ok(RustRaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload,
        })
    }

    fn snapshot(&self) -> Result<RustRaftSnapshotChunk, rustraft::RustRaftError> {
        Ok(RustRaftSnapshotChunk {
            meta: RustRaftSnapshotMeta {
                snapshot_id: "opaque".to_string(),
                last_log_id: RustRaftLogId { term: 1, index: 1 },
                membership: vec![1, 2, 3],
            },
            offset: 0,
            data: self.applied.concat(),
            done: true,
        })
    }

    fn install_snapshot(
        &mut self,
        chunk: RustRaftSnapshotChunk,
    ) -> Result<(), rustraft::RustRaftError> {
        self.applied = vec![chunk.data];
        Ok(())
    }
}

#[test]
fn generic_apply_trait_accepts_temporalstore_data_shard_payloads() {
    let mut state_machine = DataShardStateMachine::default();
    let response = rustraft_apply_entry(
        &mut state_machine,
        "tenant-a/shard-7".to_string(),
        RaftLogEntry {
            log_id: RustRaftLogId { term: 3, index: 11 },
            payload: DataShardPayload {
                key: "temperature".to_string(),
                value: b"72".to_vec(),
            },
        },
    )
    .expect("apply entry");

    assert_eq!(response.applied_index, 11);
    assert_eq!(response.response, b"72");
    assert_eq!(
        state_machine
            .snapshot("tenant-a/shard-7".to_string())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn generic_apply_trait_accepts_temporalstore_meta_payloads() {
    let mut state_machine = MetaStateMachine::default();
    let response = rustraft_apply_entry(
        &mut state_machine,
        42,
        RaftLogEntry {
            log_id: RustRaftLogId { term: 4, index: 8 },
            payload: MetaPayload {
                assignment: "shard-7 -> node-2".to_string(),
            },
        },
    )
    .expect("apply entry");

    assert_eq!(response.applied_index, 8);
    assert_eq!(response.response, "shard-7 -> node-2");
    assert_eq!(state_machine.assignments.len(), 1);
}

#[test]
fn opaque_bytes_state_machine_still_uses_compatibility_trait() {
    let mut state_machine = OpaqueBytesStateMachine::default();
    let response = rustraft_apply_entry(
        &mut state_machine,
        7,
        RaftLogEntry {
            log_id: RustRaftLogId { term: 1, index: 2 },
            payload: b"opaque temporalstore command bytes".to_vec(),
        },
    )
    .expect("apply entry");

    assert_eq!(response.applied_index, 2);
    assert_eq!(response.response, b"opaque temporalstore command bytes");
}
