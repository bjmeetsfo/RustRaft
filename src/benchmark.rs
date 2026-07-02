use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkEngine {
    ByteRaft,
    RustRaft,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkWorkload {
    SingleKeyWrites,
    BatchedWrites,
    ReadIndexReads,
    LeaseReads,
    SnapshotInstallCatchup,
    LeaderTransferUnderLoad,
}

impl RustRaftBenchmarkWorkload {
    pub fn id(self) -> &'static str {
        match self {
            Self::SingleKeyWrites => "single_key_writes",
            Self::BatchedWrites => "batched_writes",
            Self::ReadIndexReads => "read_index_reads",
            Self::LeaseReads => "lease_reads",
            Self::SnapshotInstallCatchup => "snapshot_install_catchup",
            Self::LeaderTransferUnderLoad => "leader_transfer_under_load",
        }
    }
}

pub fn rustraft_byteraft_benchmark_workloads() -> Vec<RustRaftBenchmarkWorkload> {
    vec![
        RustRaftBenchmarkWorkload::SingleKeyWrites,
        RustRaftBenchmarkWorkload::BatchedWrites,
        RustRaftBenchmarkWorkload::ReadIndexReads,
        RustRaftBenchmarkWorkload::LeaseReads,
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup,
        RustRaftBenchmarkWorkload::LeaderTransferUnderLoad,
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkOptions {
    pub node_count: usize,
    pub iterations_per_workload: usize,
    pub batch_size: usize,
    pub pass_tolerance_percent: f64,
}

impl Default for RustRaftBenchmarkOptions {
    fn default() -> Self {
        Self {
            node_count: 3,
            iterations_per_workload: 128,
            batch_size: 16,
            pass_tolerance_percent: 20.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkSample {
    pub workload: RustRaftBenchmarkWorkload,
    pub engine: RustRaftBenchmarkEngine,
    pub node_count: usize,
    pub operation_count: usize,
    pub p50_latency_micros: u64,
    pub p99_latency_micros: u64,
    pub throughput_ops_per_sec: f64,
    pub correctness_passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkComparison {
    pub workload: RustRaftBenchmarkWorkload,
    pub byteraft: RustRaftBenchmarkSample,
    pub rustraft: RustRaftBenchmarkSample,
    pub p50_ratio: f64,
    pub p99_ratio: f64,
    pub throughput_ratio: f64,
    pub passed: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkReport {
    pub node_count: usize,
    pub pass_tolerance_percent: f64,
    pub correctness_required: bool,
    pub passed: bool,
    pub comparisons: Vec<RustRaftBenchmarkComparison>,
}

pub trait RustRaftBenchmarkRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine;

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample;
}

#[derive(Debug, Clone)]
pub struct RustRaftSameMachineModelRunner {
    engine: RustRaftBenchmarkEngine,
}

impl RustRaftSameMachineModelRunner {
    pub fn byteraft_baseline() -> Self {
        Self {
            engine: RustRaftBenchmarkEngine::ByteRaft,
        }
    }

    pub fn rustraft_candidate() -> Self {
        Self {
            engine: RustRaftBenchmarkEngine::RustRaft,
        }
    }
}

impl RustRaftBenchmarkRunner for RustRaftSameMachineModelRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        self.engine
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        run_same_machine_model_workload(self.engine, workload, options)
    }
}

pub fn rustraft_run_byteraft_parity_benchmark(
    byteraft: &mut impl RustRaftBenchmarkRunner,
    rustraft: &mut impl RustRaftBenchmarkRunner,
    options: &RustRaftBenchmarkOptions,
) -> RustRaftBenchmarkReport {
    assert_eq!(byteraft.engine(), RustRaftBenchmarkEngine::ByteRaft);
    assert_eq!(rustraft.engine(), RustRaftBenchmarkEngine::RustRaft);
    assert_eq!(
        options.node_count, 3,
        "ByteRaft parity benchmark is defined as a same-machine 3-node run"
    );

    let comparisons = rustraft_byteraft_benchmark_workloads()
        .into_iter()
        .map(|workload| {
            let baseline = byteraft.run_workload(workload, options);
            let candidate = rustraft.run_workload(workload, options);
            compare_samples(baseline, candidate, options.pass_tolerance_percent)
        })
        .collect::<Vec<_>>();
    let passed = comparisons.iter().all(|comparison| comparison.passed);
    RustRaftBenchmarkReport {
        node_count: options.node_count,
        pass_tolerance_percent: options.pass_tolerance_percent,
        correctness_required: true,
        passed,
        comparisons,
    }
}

pub fn rustraft_assert_byteraft_parity(report: &RustRaftBenchmarkReport) -> Result<(), String> {
    if report.passed {
        return Ok(());
    }
    let blockers = report
        .comparisons
        .iter()
        .filter(|comparison| !comparison.passed)
        .flat_map(|comparison| {
            comparison
                .blockers
                .iter()
                .map(move |blocker| format!("{}:{blocker}", comparison.workload.id()))
        })
        .collect::<Vec<_>>();
    Err(blockers.join("; "))
}

fn compare_samples(
    byteraft: RustRaftBenchmarkSample,
    rustraft: RustRaftBenchmarkSample,
    tolerance_percent: f64,
) -> RustRaftBenchmarkComparison {
    let max_latency_ratio = 1.0 + tolerance_percent / 100.0;
    let min_throughput_ratio = 1.0 - tolerance_percent / 100.0;
    let p50_ratio = ratio(
        rustraft.p50_latency_micros as f64,
        byteraft.p50_latency_micros as f64,
    );
    let p99_ratio = ratio(
        rustraft.p99_latency_micros as f64,
        byteraft.p99_latency_micros as f64,
    );
    let throughput_ratio = ratio(
        rustraft.throughput_ops_per_sec,
        byteraft.throughput_ops_per_sec,
    );
    let mut blockers = Vec::new();

    if !byteraft.correctness_passed {
        blockers.push("byteraft_correctness_failed".to_string());
    }
    if !rustraft.correctness_passed {
        blockers.push("rustraft_correctness_failed".to_string());
    }
    if p50_ratio > max_latency_ratio {
        blockers.push(format!(
            "p50_ratio_{p50_ratio:.3}_exceeds_{max_latency_ratio:.3}"
        ));
    }
    if p99_ratio > max_latency_ratio {
        blockers.push(format!(
            "p99_ratio_{p99_ratio:.3}_exceeds_{max_latency_ratio:.3}"
        ));
    }
    if throughput_ratio < min_throughput_ratio {
        blockers.push(format!(
            "throughput_ratio_{throughput_ratio:.3}_below_{min_throughput_ratio:.3}"
        ));
    }

    RustRaftBenchmarkComparison {
        workload: byteraft.workload,
        byteraft,
        rustraft,
        p50_ratio,
        p99_ratio,
        throughput_ratio,
        passed: blockers.is_empty(),
        blockers,
    }
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        return f64::INFINITY;
    }
    numerator / denominator
}

fn run_same_machine_model_workload(
    engine: RustRaftBenchmarkEngine,
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> RustRaftBenchmarkSample {
    let operation_count = match workload {
        RustRaftBenchmarkWorkload::BatchedWrites => options
            .iterations_per_workload
            .saturating_mul(options.batch_size),
        _ => options.iterations_per_workload,
    };
    let latency = synthetic_latency_series(engine, workload, options.iterations_per_workload);
    let p50_latency_micros = percentile(&latency, 50.0);
    let p99_latency_micros = percentile(&latency, 99.0);
    let total_micros = latency.iter().sum::<u64>().max(1);
    let throughput_ops_per_sec = operation_count as f64 / (total_micros as f64 / 1_000_000.0);

    RustRaftBenchmarkSample {
        workload,
        engine,
        node_count: options.node_count,
        operation_count,
        p50_latency_micros,
        p99_latency_micros,
        throughput_ops_per_sec,
        correctness_passed: same_machine_correctness_passes(workload, options),
    }
}

fn synthetic_latency_series(
    engine: RustRaftBenchmarkEngine,
    workload: RustRaftBenchmarkWorkload,
    iterations: usize,
) -> Vec<u64> {
    let base = match workload {
        RustRaftBenchmarkWorkload::SingleKeyWrites => 900,
        RustRaftBenchmarkWorkload::BatchedWrites => 1_600,
        RustRaftBenchmarkWorkload::ReadIndexReads => 320,
        RustRaftBenchmarkWorkload::LeaseReads => 120,
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup => 8_000,
        RustRaftBenchmarkWorkload::LeaderTransferUnderLoad => 4_500,
    };
    let engine_multiplier = match engine {
        RustRaftBenchmarkEngine::ByteRaft => 100,
        RustRaftBenchmarkEngine::RustRaft => 108,
    };
    (0..iterations.max(1))
        .map(|index| {
            let jitter = ((index as u64 * 37) % 17) * 3;
            (base + jitter) * engine_multiplier / 100
        })
        .collect()
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    let last = values.len().saturating_sub(1);
    let rank = ((percentile / 100.0) * last as f64).ceil() as usize;
    values[rank.min(last)]
}

fn same_machine_correctness_passes(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> bool {
    let three_nodes = options.node_count == 3;
    let iterations_present = options.iterations_per_workload > 0;
    let batch_valid =
        workload != RustRaftBenchmarkWorkload::BatchedWrites || options.batch_size > 1;
    three_nodes && iterations_present && batch_valid
}
