use rustraft::{
    rustraft_wal_lifecycle_evidence, PersistentRaftWal, PersistentRaftWalOptions, RaftWalRecord,
    RustRaftApplySnapshotFence, RustRaftHardState, RustRaftLogId, RustRaftMembership,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_wal_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("rustraft-{name}-{}-{nonce}", std::process::id()))
}

fn wal_options(dir: PathBuf) -> PersistentRaftWalOptions {
    PersistentRaftWalOptions {
        dir,
        max_records_per_segment: 2,
        max_segment_bytes: 4096,
        min_keep_segments: 1,
        fsync_on_append: true,
    }
}

fn wal_record(index: u64) -> RaftWalRecord {
    RaftWalRecord {
        group_id: 9,
        node_id: 1,
        hard_state: RustRaftHardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(RustRaftLogId { term: 3, index }),
        },
        membership: RustRaftMembership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: Vec::new(),
        installed_snapshot: None,
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

#[test]
fn persistent_wal_rolls_segments_and_recovers_after_restart() {
    let dir = temp_wal_dir("restart");
    let options = wal_options(dir.clone());
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open wal");
        wal.append(wal_record(1)).expect("append 1");
        wal.append(wal_record(2)).expect("append 2");
        wal.append(wal_record(3)).expect("append 3");
        assert_eq!(wal.status().segment_count, 2);
        assert_eq!(wal.status().last_log_index, 3);
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen wal");
    let report = reopened.recover().expect("recover wal");
    assert!(!report.truncated_corrupt_tail);
    assert_eq!(
        report
            .recovered
            .expect("latest")
            .hard_state
            .committed
            .expect("commit")
            .index,
        3
    );
    assert_eq!(reopened.records().len(), 3);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_truncates_corrupt_tail_on_recovery() {
    let dir = temp_wal_dir("corrupt-tail");
    let options = wal_options(dir.clone());
    {
        let mut wal = PersistentRaftWal::open(options.clone()).expect("open wal");
        wal.append(wal_record(1)).expect("append 1");
        wal.append(wal_record(2)).expect("append 2");
        wal.corrupt_tail_for_test().expect("corrupt tail");
    }

    let mut reopened = PersistentRaftWal::open(options).expect("reopen wal");
    let report = reopened.recover().expect("recover");
    assert!(report.truncated_corrupt_tail);
    assert_eq!(report.surviving_records, 2);
    assert_eq!(reopened.records().len(), 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn persistent_wal_compacts_released_segments_and_reports_lifecycle_evidence() {
    let dir = temp_wal_dir("compact");
    let options = wal_options(dir.clone());
    let mut wal = PersistentRaftWal::open(options).expect("open wal");
    for index in 1..=5 {
        wal.append(wal_record(index)).expect("append");
    }
    assert_eq!(wal.status().segment_count, 3);

    let released = wal.compact_through(4).expect("compact");
    assert_eq!(released, 2);
    let status = wal.status();
    assert_eq!(status.segment_count, 1);
    assert_eq!(status.first_log_index, 5);
    assert_eq!(status.released_segment_count, 2);
    let evidence = rustraft_wal_lifecycle_evidence(&status);
    assert!(evidence.segment_lifecycle_present);
    assert!(evidence.compaction_observed);

    let mut reopened = PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: dir.clone(),
        max_records_per_segment: 2,
        max_segment_bytes: 4096,
        min_keep_segments: 1,
        fsync_on_append: true,
    })
    .expect("reopen compacted");
    let report = reopened.recover().expect("recover compacted");
    assert_eq!(
        report
            .recovered
            .expect("latest")
            .hard_state
            .committed
            .expect("commit")
            .index,
        5
    );

    let _ = fs::remove_dir_all(dir);
}
