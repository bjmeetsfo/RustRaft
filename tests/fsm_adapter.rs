use rustraft::{
    RaftApply, RaftApplyRequest, RaftApplyResponse, RaftFsmAdapter, RaftLogEntry, RaftStateMachine,
    RustRaftLogId,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CountingFsm {
    applied: Vec<(u64, Vec<u8>)>,
}

impl RaftApply<u64, Vec<u8>> for CountingFsm {
    type Response = Vec<u8>;

    fn apply(
        &mut self,
        request: RaftApplyRequest<u64, Vec<u8>>,
    ) -> Result<RaftApplyResponse<Self::Response>, rustraft::RaftError> {
        self.applied
            .push((request.log_id.index, request.payload.clone()));
        Ok(RaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload,
        })
    }
}

impl RaftStateMachine<u64, Vec<u8>> for CountingFsm {
    type Snapshot = Vec<(u64, Vec<u8>)>;

    fn snapshot(&self, _group_id: u64) -> Result<Self::Snapshot, rustraft::RaftError> {
        Ok(self.applied.clone())
    }

    fn install_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), rustraft::RaftError> {
        self.applied = snapshot;
        Ok(())
    }
}

fn entry(term: u64, index: u64, payload: &[u8]) -> RaftLogEntry<Vec<u8>> {
    RaftLogEntry {
        log_id: RustRaftLogId { term, index },
        payload: payload.to_vec(),
    }
}

#[test]
fn fsm_adapter_applies_duplicate_log_id_idempotently() {
    let mut adapter = RaftFsmAdapter::new(7, CountingFsm::default());

    let first = adapter
        .apply_entry(entry(1, 1, b"set-a"))
        .expect("first apply");
    assert!(first.applied);
    assert!(!first.replayed);

    let duplicate = adapter
        .apply_entry(entry(1, 1, b"set-a"))
        .expect("duplicate replay");
    assert!(!duplicate.applied);
    assert!(duplicate.replayed);
    assert_eq!(duplicate.response.response, b"set-a");
    assert_eq!(adapter.inner().applied.len(), 1);
    assert_eq!(adapter.last_applied(), 1);
}

#[test]
fn fsm_adapter_rejects_conflicting_replay_at_same_index() {
    let mut adapter = RaftFsmAdapter::new(7, CountingFsm::default());
    adapter
        .apply_entry(entry(1, 1, b"term-one"))
        .expect("first apply");

    let err = adapter
        .apply_entry(entry(2, 1, b"term-two"))
        .expect_err("conflict rejected");
    assert!(err.to_string().contains("FSM replay conflict"));
    assert_eq!(adapter.inner().applied.len(), 1);
}

#[test]
fn fsm_adapter_replay_report_tracks_applied_and_skipped_entries() {
    let mut adapter = RaftFsmAdapter::new(7, CountingFsm::default());
    let report = adapter
        .replay_entries(vec![
            entry(1, 1, b"a"),
            entry(1, 2, b"b"),
            entry(1, 2, b"b"),
            entry(1, 3, b"c"),
        ])
        .expect("replay");

    assert_eq!(report.attempted, 4);
    assert_eq!(report.applied, 3);
    assert_eq!(report.skipped_replay, 1);
    assert_eq!(report.last_applied, 3);
    assert!(report.idempotent);
    assert_eq!(adapter.inner().applied.len(), 3);
}

#[test]
fn fsm_adapter_checkpoints_and_loads_state_machine_snapshot() {
    let mut adapter = RaftFsmAdapter::new(7, CountingFsm::default());
    adapter
        .replay_entries(vec![entry(1, 1, b"a"), entry(1, 2, b"b")])
        .expect("initial replay");

    let checkpoint = adapter.checkpoint().expect("checkpoint");
    assert_eq!(checkpoint.last_applied, 2);
    assert_eq!(checkpoint.applied_log_ids.len(), 2);
    assert_eq!(checkpoint.snapshot.len(), 2);

    let mut restored = RaftFsmAdapter::new(7, CountingFsm::default());
    restored
        .install_checkpoint(checkpoint)
        .expect("install checkpoint");
    assert_eq!(restored.inner().applied.len(), 2);
    assert_eq!(restored.applied_log_count(), 2);
    assert_eq!(restored.last_applied(), 2);

    let report = restored
        .replay_entries(vec![entry(1, 3, b"c")])
        .expect("tail replay");
    assert_eq!(report.applied, 1);
    assert_eq!(restored.inner().applied.len(), 3);
}
