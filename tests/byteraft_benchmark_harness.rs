use rustraft::benchmark::{
    rustraft_assert_byteraft_parity, rustraft_assert_production_byteraft_parity,
    rustraft_byteraft_benchmark_evidence, rustraft_byteraft_benchmark_workloads,
    rustraft_run_byteraft_parity_benchmark, RustRaftBenchmarkEngine, RustRaftBenchmarkEngineSource,
    RustRaftBenchmarkOptions, RustRaftBenchmarkRunner, RustRaftBenchmarkSample,
    RustRaftBenchmarkWorkload, RustRaftRuntimeBenchmarkRunner, RustRaftSameMachineModelRunner,
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
fn external_benchmark_script_runs_outside_temporalstore() {
    let script = include_str!("../scripts/byteraft_vs_rustraft_benchmark.sh");

    assert!(script.contains("--manifest-path \"$rustraft_root/Cargo.toml\""));
    assert!(script.contains("--example byteraft_parity_benchmark"));
    assert!(script.contains("BYTERAFT_BENCHMARK_BIN"));
    assert!(script.contains("benchmark:real_byteraft_missing"));
    assert!(script.contains("BYTERAFT_ROOT"));
    assert!(script.contains("BENCHMARK_OUT"));
    assert!(script.contains("build_byteraft_parity_benchmark.sh"));
    assert!(script.contains("--target byteraft_parity_benchmark"));
    assert!(script.contains("bazel build //:byteraft_parity_benchmark"));
    assert!(!script.contains("TemporalStore.git"));
    assert!(!script.contains("crates/temporalstore-rust"));
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
    let error = rustraft_assert_production_byteraft_parity(&report).unwrap_err();
    assert!(error.contains("benchmark:real_byteraft_missing"));
    assert!(error.contains("benchmark:model_rustraft"));
}

#[test]
fn parity_gate_fails_when_correctness_or_perf_regresses() {
    let options = RustRaftBenchmarkOptions::default();
    let mut byteraft = FixedRunner::new(
        RustRaftBenchmarkEngine::ByteRaft,
        RustRaftBenchmarkEngineSource::RealByteRaft,
        true,
        1_000,
        2_000,
        1_000.0,
    );
    let mut rustraft = FixedRunner::new(
        RustRaftBenchmarkEngine::RustRaft,
        RustRaftBenchmarkEngineSource::RustRaftRuntime,
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

#[test]
fn production_parity_accepts_real_byteraft_and_rustraft_runtime_sources() {
    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: 4,
        batch_size: 2,
        ..Default::default()
    };
    let mut byteraft = FixedRunner::new(
        RustRaftBenchmarkEngine::ByteRaft,
        RustRaftBenchmarkEngineSource::RealByteRaft,
        true,
        1_000,
        2_000,
        1_000.0,
    );
    let mut rustraft = FixedRunner::new(
        RustRaftBenchmarkEngine::RustRaft,
        RustRaftBenchmarkEngineSource::RustRaftRuntime,
        true,
        1_100,
        2_100,
        900.0,
    );
    let report = rustraft_run_byteraft_parity_benchmark(&mut byteraft, &mut rustraft, &options);
    rustraft_assert_production_byteraft_parity(&report).unwrap();
    let evidence = rustraft_byteraft_benchmark_evidence(&report);
    assert!(evidence.real_byteraft);
    assert!(evidence.rustraft_runtime);
    assert!(evidence.correctness_passed);
    assert!(evidence.performance_within_threshold);
    assert_eq!(evidence.workloads.len(), 9);
}

#[test]
fn rustraft_runtime_runner_uses_runtime_source_not_model_source() {
    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: 2,
        batch_size: 2,
        ..Default::default()
    };
    let mut runner = RustRaftRuntimeBenchmarkRunner::new("test");
    let sample = runner.run_workload(RustRaftBenchmarkWorkload::SingleKeyWrites, &options);
    assert_eq!(sample.engine, RustRaftBenchmarkEngine::RustRaft);
    assert_eq!(
        sample.engine_source,
        RustRaftBenchmarkEngineSource::RustRaftRuntime
    );
    assert!(sample.correctness_passed);
    assert_eq!(sample.node_count, 3);
}

struct FixedRunner {
    engine: RustRaftBenchmarkEngine,
    source: RustRaftBenchmarkEngineSource,
    correctness_passed: bool,
    p50_latency_micros: u64,
    p99_latency_micros: u64,
    throughput_ops_per_sec: f64,
}

impl FixedRunner {
    fn new(
        engine: RustRaftBenchmarkEngine,
        source: RustRaftBenchmarkEngineSource,
        correctness_passed: bool,
        p50_latency_micros: u64,
        p99_latency_micros: u64,
        throughput_ops_per_sec: f64,
    ) -> Self {
        Self {
            engine,
            source,
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

    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        self.source
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        RustRaftBenchmarkSample {
            workload,
            engine: self.engine,
            engine_source: self.source,
            binary_path: Some(format!("{:?}-benchmark", self.engine)),
            git_revision: Some("test-revision".to_string()),
            build_profile: "test".to_string(),
            node_count: options.node_count,
            operation_count: options.iterations_per_workload,
            p50_latency_micros: self.p50_latency_micros,
            p99_latency_micros: self.p99_latency_micros,
            throughput_ops_per_sec: self.throughput_ops_per_sec,
            correctness_passed: self.correctness_passed,
        }
    }
}
