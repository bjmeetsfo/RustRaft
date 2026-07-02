use rustraft::benchmark::{
    rustraft_assert_byteraft_parity, rustraft_byteraft_benchmark_workloads,
    rustraft_run_byteraft_parity_benchmark, RustRaftBenchmarkEngine, RustRaftBenchmarkOptions,
    RustRaftBenchmarkRunner, RustRaftBenchmarkSample, RustRaftBenchmarkWorkload,
    RustRaftSameMachineModelRunner,
};

#[test]
fn same_machine_benchmark_covers_required_byteraft_workloads() {
    let workloads = rustraft_byteraft_benchmark_workloads()
        .into_iter()
        .map(|workload| workload.id())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        workloads,
        [
            "single_key_writes",
            "batched_writes",
            "replication_batching",
            "wal_fsync",
            "read_index_reads",
            "lease_reads",
            "snapshot_install_catchup",
            "snapshot_streaming",
            "leader_transfer_under_load",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
    );
}

#[test]
fn same_machine_model_passes_twenty_percent_parity_gate() {
    let options = RustRaftBenchmarkOptions::default();
    let mut byteraft = RustRaftSameMachineModelRunner::byteraft_baseline();
    let mut rustraft = RustRaftSameMachineModelRunner::rustraft_candidate();
    let report = rustraft_run_byteraft_parity_benchmark(&mut byteraft, &mut rustraft, &options);

    assert!(report.passed, "{report:#?}");
    assert_eq!(report.comparisons.len(), 9);
    for comparison in &report.comparisons {
        assert!(comparison.byteraft.correctness_passed);
        assert!(comparison.rustraft.correctness_passed);
        assert!(comparison.p50_ratio <= 1.2, "{comparison:#?}");
        assert!(comparison.p99_ratio <= 1.2, "{comparison:#?}");
        assert!(comparison.throughput_ratio >= 0.8, "{comparison:#?}");
    }
    rustraft_assert_byteraft_parity(&report).unwrap();
}

#[test]
fn parity_gate_fails_when_correctness_or_perf_regresses() {
    let options = RustRaftBenchmarkOptions::default();
    let mut byteraft = FixedRunner::new(
        RustRaftBenchmarkEngine::ByteRaft,
        true,
        1_000,
        2_000,
        1_000.0,
    );
    let mut rustraft = FixedRunner::new(
        RustRaftBenchmarkEngine::RustRaft,
        false,
        1_300,
        2_500,
        700.0,
    );

    let report = rustraft_run_byteraft_parity_benchmark(&mut byteraft, &mut rustraft, &options);
    let error = rustraft_assert_byteraft_parity(&report).unwrap_err();

    assert!(!report.passed);
    assert!(error.contains("rustraft_correctness_failed"));
    assert!(error.contains("p50_ratio"));
    assert!(error.contains("p99_ratio"));
    assert!(error.contains("throughput_ratio"));
}

struct FixedRunner {
    engine: RustRaftBenchmarkEngine,
    correctness_passed: bool,
    p50_latency_micros: u64,
    p99_latency_micros: u64,
    throughput_ops_per_sec: f64,
}

impl FixedRunner {
    fn new(
        engine: RustRaftBenchmarkEngine,
        correctness_passed: bool,
        p50_latency_micros: u64,
        p99_latency_micros: u64,
        throughput_ops_per_sec: f64,
    ) -> Self {
        Self {
            engine,
            correctness_passed,
            p50_latency_micros,
            p99_latency_micros,
            throughput_ops_per_sec,
        }
    }
}

impl RustRaftBenchmarkRunner for FixedRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        self.engine
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        RustRaftBenchmarkSample {
            workload,
            engine: self.engine,
            node_count: options.node_count,
            operation_count: options.iterations_per_workload,
            p50_latency_micros: self.p50_latency_micros,
            p99_latency_micros: self.p99_latency_micros,
            throughput_ops_per_sec: self.throughput_ops_per_sec,
            correctness_passed: self.correctness_passed,
        }
    }
}
