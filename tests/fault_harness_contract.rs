use rustraft::fault::{
    rustraft_byteraft_fault_scenarios, rustraft_fault_harness_readiness_report,
    RustRaftFaultScenario, RustRaftFaultScenarioEvidence,
};

fn passing_evidence(scenario: RustRaftFaultScenario) -> RustRaftFaultScenarioEvidence {
    RustRaftFaultScenarioEvidence {
        scenario,
        process_path_observed: true,
        independent_wal_dirs_observed: true,
        independent_snapshot_dirs_observed: true,
        safety_observed: true,
        recovery_observed: true,
        metrics_observed: true,
        report_path: Some(format!("reports/{}.json", scenario.id())),
    }
}

#[test]
fn byteraft_fault_contract_names_required_process_scenarios() {
    let scenarios = rustraft_byteraft_fault_scenarios()
        .into_iter()
        .map(|item| item.scenario.id())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        scenarios,
        [
            "packet_loss_majority",
            "slow_wal_fsync",
            "snapshot_during_membership_change",
            "leader_transfer_under_load",
            "follower_rejoin_compacted_logs",
            "rolling_restart_joint_consensus",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    );
}

#[test]
fn fault_harness_readiness_fails_closed_on_missing_process_evidence() {
    let report = rustraft_fault_harness_readiness_report(&[]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"packet_loss_majority:evidence_missing".to_string()));
    assert!(report
        .missing
        .contains(&"rolling_restart_joint_consensus:evidence_missing".to_string()));
}

#[test]
fn fault_harness_readiness_requires_independent_stores_safety_recovery_and_metrics() {
    let report = rustraft_fault_harness_readiness_report(&[RustRaftFaultScenarioEvidence {
        scenario: RustRaftFaultScenario::PacketLossMajority,
        process_path_observed: true,
        independent_wal_dirs_observed: false,
        independent_snapshot_dirs_observed: false,
        safety_observed: false,
        recovery_observed: false,
        metrics_observed: false,
        report_path: Some("reports/packet-loss.json".to_string()),
    }]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"packet_loss_majority:independent_wal_dirs_observed".to_string()));
    assert!(report
        .missing
        .contains(&"packet_loss_majority:safety_observed".to_string()));
    assert!(report
        .missing
        .contains(&"packet_loss_majority:metrics_observed".to_string()));
}

#[test]
fn fault_harness_readiness_accepts_complete_byteraft_style_evidence() {
    let evidence = rustraft_byteraft_fault_scenarios()
        .into_iter()
        .map(|requirement| passing_evidence(requirement.scenario))
        .collect::<Vec<_>>();
    let report = rustraft_fault_harness_readiness_report(&evidence);

    assert!(report.ready, "{report:#?}");
    assert!(report.missing.is_empty());
    assert!(report.results.iter().all(|result| result.ready));
}

#[test]
fn leader_transfer_under_load_requires_exact_once_report_path() {
    let mut evidence = passing_evidence(RustRaftFaultScenario::LeaderTransferUnderLoad);
    evidence.report_path = None;

    let report = rustraft_fault_harness_readiness_report(&[evidence]);

    assert!(!report.ready);
    assert!(report
        .missing
        .contains(&"leader_transfer_under_load:exact_once_report_path".to_string()));
}
