use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{
    PersistentRaftWal, PersistentRaftWalOptions, RaftCluster, RaftConfig, RaftError,
    RustRaftApplySnapshotFence, RustRaftHardState, RustRaftLogEntry, RustRaftLogId,
    RustRaftMembership, RustRaftPeer, RustRaftReplicaRole, RustRaftSnapshotMeta, RustRaftWalRecord,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkEngine {
    ByteRaft,
    RustRaft,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkEngineSource {
    RealByteRaft,
    RustRaftRuntime,
    Model,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkWorkload {
    SingleKeyWrites,
    BatchedWrites,
    ReplicationBatching,
    WalFsync,
    ReadIndexReads,
    LeaseReads,
    SnapshotInstallCatchup,
    SnapshotStreaming,
    LeaderTransferUnderLoad,
}

impl RustRaftBenchmarkWorkload {
    pub fn id(self) -> &'static str {
        match self {
            Self::SingleKeyWrites => "single_key_writes",
            Self::BatchedWrites => "batched_writes",
            Self::ReplicationBatching => "replication_batching",
            Self::WalFsync => "wal_fsync",
            Self::ReadIndexReads => "read_index_reads",
            Self::LeaseReads => "lease_reads",
            Self::SnapshotInstallCatchup => "snapshot_install_catchup",
            Self::SnapshotStreaming => "snapshot_streaming",
            Self::LeaderTransferUnderLoad => "leader_transfer_under_load",
        }
    }
}

pub fn rustraft_byteraft_benchmark_workloads() -> Vec<RustRaftBenchmarkWorkload> {
    vec![
        RustRaftBenchmarkWorkload::SingleKeyWrites,
        RustRaftBenchmarkWorkload::BatchedWrites,
        RustRaftBenchmarkWorkload::ReplicationBatching,
        RustRaftBenchmarkWorkload::WalFsync,
        RustRaftBenchmarkWorkload::ReadIndexReads,
        RustRaftBenchmarkWorkload::LeaseReads,
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup,
        RustRaftBenchmarkWorkload::SnapshotStreaming,
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
    pub engine_source: RustRaftBenchmarkEngineSource,
    #[serde(default)]
    pub binary_path: Option<String>,
    #[serde(default)]
    pub git_revision: Option<String>,
    #[serde(default)]
    pub build_profile: String,
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
    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        RustRaftBenchmarkEngineSource::Model
    }
    fn binary_path(&self) -> Option<String> {
        None
    }
    fn git_revision(&self) -> Option<String> {
        None
    }
    fn build_profile(&self) -> String {
        "test".to_string()
    }

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

#[derive(Debug, Clone)]
pub struct RustRaftRuntimeBenchmarkRunner {
    build_profile: String,
    git_revision: Option<String>,
}

impl RustRaftRuntimeBenchmarkRunner {
    pub fn new(build_profile: impl Into<String>) -> Self {
        Self {
            build_profile: build_profile.into(),
            git_revision: option_env!("VERGEN_GIT_SHA")
                .or(option_env!("GIT_HASH"))
                .map(str::to_string),
        }
    }
}

impl Default for RustRaftRuntimeBenchmarkRunner {
    fn default() -> Self {
        Self::new("debug")
    }
}

impl RustRaftBenchmarkRunner for RustRaftRuntimeBenchmarkRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        RustRaftBenchmarkEngine::RustRaft
    }

    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        RustRaftBenchmarkEngineSource::RustRaftRuntime
    }

    fn binary_path(&self) -> Option<String> {
        std::env::current_exe()
            .ok()
            .map(|path| path.display().to_string())
    }

    fn git_revision(&self) -> Option<String> {
        self.git_revision.clone()
    }

    fn build_profile(&self) -> String {
        self.build_profile.clone()
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        run_rustraft_runtime_workload(workload, options, self)
    }
}

#[derive(Debug, Clone)]
pub struct RustRaftExternalByteRaftRunner {
    binary_path: PathBuf,
    byteraft_root: Option<PathBuf>,
    git_revision: Option<String>,
    build_profile: String,
}

impl RustRaftExternalByteRaftRunner {
    pub fn new(
        binary_path: impl Into<PathBuf>,
        byteraft_root: Option<impl Into<PathBuf>>,
        build_profile: impl Into<String>,
    ) -> Result<Self, String> {
        let binary_path = binary_path.into();
        if !binary_path.is_file() {
            return Err(format!(
                "benchmark:real_byteraft_missing:{}",
                binary_path.display()
            ));
        }
        let byteraft_root = byteraft_root.map(Into::into);
        let git_revision = byteraft_root
            .as_ref()
            .and_then(|root| git_revision_for(root).ok());
        Ok(Self {
            binary_path,
            byteraft_root,
            git_revision,
            build_profile: build_profile.into(),
        })
    }

    pub fn from_root(
        byteraft_root: impl AsRef<Path>,
        build_profile: impl Into<String>,
    ) -> Result<Self, String> {
        let root = byteraft_root.as_ref();
        let binary = rustraft_find_byteraft_harness(root)?;
        Self::new(binary, Some(root.to_path_buf()), build_profile)
    }
}

impl RustRaftBenchmarkRunner for RustRaftExternalByteRaftRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        RustRaftBenchmarkEngine::ByteRaft
    }

    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        RustRaftBenchmarkEngineSource::RealByteRaft
    }

    fn binary_path(&self) -> Option<String> {
        Some(self.binary_path.display().to_string())
    }

    fn git_revision(&self) -> Option<String> {
        self.git_revision.clone()
    }

    fn build_profile(&self) -> String {
        self.build_profile.clone()
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        let mut command = Command::new(&self.binary_path);
        command
            .arg("--workload")
            .arg(workload.id())
            .arg("--node-count")
            .arg(options.node_count.to_string())
            .arg("--iterations")
            .arg(options.iterations_per_workload.to_string())
            .arg("--batch-size")
            .arg(options.batch_size.to_string());
        if let Some(root) = &self.byteraft_root {
            command.arg("--byteraft-root").arg(root);
        }
        let output = command.output().unwrap_or_else(|err| {
            panic!(
                "failed to run real ByteRaft benchmark harness {}: {err}",
                self.binary_path.display()
            )
        });
        if !output.status.success() {
            panic!(
                "real ByteRaft benchmark harness failed for {}: {}",
                workload.id(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let mut sample: RustRaftBenchmarkSample = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|err| {
                panic!(
                    "real ByteRaft benchmark harness emitted invalid JSON for {}: {err}; stdout={}",
                    workload.id(),
                    String::from_utf8_lossy(&output.stdout)
                )
            });
        sample.workload = workload;
        sample.engine = RustRaftBenchmarkEngine::ByteRaft;
        sample.engine_source = RustRaftBenchmarkEngineSource::RealByteRaft;
        sample.binary_path = self.binary_path();
        sample.git_revision = self.git_revision();
        sample.build_profile = self.build_profile();
        sample
    }
}

pub fn rustraft_find_byteraft_harness(byteraft_root: impl AsRef<Path>) -> Result<PathBuf, String> {
    let root = byteraft_root.as_ref();
    if !root.is_dir() {
        return Err(format!(
            "benchmark:real_byteraft_missing:{}",
            root.display()
        ));
    }
    let candidates = [
        root.join("target/release/byteraft_parity_benchmark"),
        root.join("target/debug/byteraft_parity_benchmark"),
        root.join("build/byteraft_parity_benchmark"),
        root.join("bin/byteraft_parity_benchmark"),
        root.join("byteraft_parity_benchmark"),
    ];
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| format!("benchmark:real_byteraft_missing:{}", root.display()))
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

pub fn rustraft_assert_production_byteraft_parity(
    report: &RustRaftBenchmarkReport,
) -> Result<(), String> {
    rustraft_assert_byteraft_parity(report)?;
    let evidence = rustraft_byteraft_benchmark_evidence(report);
    if evidence.real_byteraft
        && evidence.rustraft_runtime
        && evidence.correctness_passed
        && evidence.performance_within_threshold
        && evidence.blockers.is_empty()
    {
        return Ok(());
    }
    Err(evidence.blockers.join("; "))
}

pub fn rustraft_byteraft_benchmark_evidence(
    report: &RustRaftBenchmarkReport,
) -> crate::RustRaftByteRaftBenchmarkEvidence {
    let mut blockers = Vec::new();
    let mut workloads = Vec::new();
    let real_byteraft = report.comparisons.iter().all(|comparison| {
        comparison.byteraft.engine_source == RustRaftBenchmarkEngineSource::RealByteRaft
    });
    let rustraft_runtime = report.comparisons.iter().all(|comparison| {
        comparison.rustraft.engine_source == RustRaftBenchmarkEngineSource::RustRaftRuntime
    });
    let correctness_passed = report.comparisons.iter().all(|comparison| {
        comparison.byteraft.correctness_passed && comparison.rustraft.correctness_passed
    });
    let performance_within_threshold = report
        .comparisons
        .iter()
        .all(|comparison| comparison.passed);
    for comparison in &report.comparisons {
        workloads.push(comparison.workload.id().to_string());
        if comparison.byteraft.engine_source == RustRaftBenchmarkEngineSource::Model {
            blockers.push(format!(
                "benchmark:model_byteraft:{}",
                comparison.workload.id()
            ));
        }
        if comparison.rustraft.engine_source == RustRaftBenchmarkEngineSource::Model {
            blockers.push(format!(
                "benchmark:model_rustraft:{}",
                comparison.workload.id()
            ));
        }
        for blocker in &comparison.blockers {
            let blocker = if blocker.starts_with("p99_ratio") {
                "benchmark:p99_regression".to_string()
            } else if blocker.starts_with("p50_ratio") {
                "benchmark:p50_regression".to_string()
            } else if blocker.starts_with("throughput_ratio") {
                "benchmark:throughput_regression".to_string()
            } else {
                format!("benchmark:{blocker}")
            };
            blockers.push(format!("{}:{}", comparison.workload.id(), blocker));
        }
    }
    if !real_byteraft {
        blockers.push("benchmark:real_byteraft_missing".to_string());
    }
    if !rustraft_runtime {
        blockers.push("benchmark:rustraft_runtime_missing".to_string());
    }
    crate::RustRaftByteRaftBenchmarkEvidence {
        real_byteraft,
        rustraft_runtime,
        correctness_passed,
        performance_within_threshold,
        workloads,
        blockers,
    }
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
        RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => options
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
        engine_source: RustRaftBenchmarkEngineSource::Model,
        binary_path: None,
        git_revision: None,
        build_profile: "model".to_string(),
        node_count: options.node_count,
        operation_count,
        p50_latency_micros,
        p99_latency_micros,
        throughput_ops_per_sec,
        correctness_passed: same_machine_correctness_passes(workload, options),
    }
}

fn run_rustraft_runtime_workload(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
    runner: &RustRaftRuntimeBenchmarkRunner,
) -> RustRaftBenchmarkSample {
    let operation_count = operation_count_for(workload, options);
    let mut latencies = Vec::new();
    let correctness_passed = match workload {
        RustRaftBenchmarkWorkload::SingleKeyWrites
        | RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => {
            let mut cluster = benchmark_cluster();
            cluster.start().is_ok()
                && run_timed(options.iterations_per_workload, &mut latencies, |_| {
                    for _ in 0..writes_per_iteration(workload, options) {
                        cluster.propose(b"runtime-write".to_vec())?;
                    }
                    Ok(())
                })
                .is_ok()
        }
        RustRaftBenchmarkWorkload::WalFsync => {
            let dir = temp_benchmark_dir("wal");
            let mut wal = PersistentRaftWal::open(PersistentRaftWalOptions {
                dir: dir.clone(),
                max_records_per_segment: 128,
                max_segment_bytes: 64 * 1024 * 1024,
                min_keep_segments: 2,
                fsync_on_append: true,
            })
            .expect("open benchmark WAL");
            let ok = run_timed(
                options.iterations_per_workload,
                &mut latencies,
                |iteration| {
                    let index = iteration as u64 + 1;
                    wal.append(benchmark_wal_record(index))?;
                    Ok(())
                },
            )
            .is_ok();
            let _ = std::fs::remove_dir_all(dir);
            ok
        }
        RustRaftBenchmarkWorkload::ReadIndexReads | RustRaftBenchmarkWorkload::LeaseReads => {
            let mut cluster = benchmark_cluster();
            let started = cluster.start().is_ok() && cluster.propose(b"seed".to_vec()).is_ok();
            started
                && run_timed(options.iterations_per_workload, &mut latencies, |_| {
                    let response = cluster.read_index(crate::RustRaftReadIndexRequest {
                        group_id: 10,
                        requester_id: 1,
                        min_commit_index: 1,
                        allow_lease_read: matches!(workload, RustRaftBenchmarkWorkload::LeaseReads),
                    })?;
                    if !response.safe {
                        return Err(RaftError::InvalidRequest(response.reason));
                    }
                    Ok(())
                })
                .is_ok()
        }
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup
        | RustRaftBenchmarkWorkload::SnapshotStreaming => {
            let mut cluster = benchmark_cluster();
            let started = cluster.start().is_ok();
            started
                && run_timed(
                    options.iterations_per_workload,
                    &mut latencies,
                    |iteration| {
                        let index = iteration as u64 + 1;
                        cluster.install_snapshot_with_tail_to(
                            2,
                            crate::RaftSnapshot {
                                group_id: 10,
                                meta: RustRaftSnapshotMeta {
                                    snapshot_id: format!("bench-snap-{index}"),
                                    last_log_id: RustRaftLogId { term: 1, index },
                                    membership: vec![1, 2, 3],
                                },
                                payload: vec![42; 1024],
                            },
                            RustRaftApplySnapshotFence {
                                applied_index: index,
                                commit_index: index,
                                installed_snapshot_index: index,
                                first_retained_log_index: index + 1,
                            },
                            Vec::new(),
                        )?;
                        Ok(())
                    },
                )
                .is_ok()
        }
        RustRaftBenchmarkWorkload::LeaderTransferUnderLoad => {
            let mut cluster = benchmark_cluster();
            let started = cluster.start().is_ok();
            started
                && run_timed(options.iterations_per_workload, &mut latencies, |_| {
                    cluster.propose(b"transfer-load".to_vec())?;
                    let target = if cluster.leader_id() == Some(1) { 2 } else { 1 };
                    cluster.transfer_leader(target)?;
                    Ok(())
                })
                .is_ok()
        }
    };
    let total_micros = latencies.iter().sum::<u64>().max(1);
    RustRaftBenchmarkSample {
        workload,
        engine: RustRaftBenchmarkEngine::RustRaft,
        engine_source: RustRaftBenchmarkEngineSource::RustRaftRuntime,
        binary_path: runner.binary_path(),
        git_revision: runner.git_revision(),
        build_profile: runner.build_profile(),
        node_count: options.node_count,
        operation_count,
        p50_latency_micros: percentile(&latencies, 50.0),
        p99_latency_micros: percentile(&latencies, 99.0),
        throughput_ops_per_sec: operation_count as f64 / (total_micros as f64 / 1_000_000.0),
        correctness_passed,
    }
}

fn run_timed(
    iterations: usize,
    latencies: &mut Vec<u64>,
    mut operation: impl FnMut(usize) -> Result<(), RaftError>,
) -> Result<(), RaftError> {
    for iteration in 0..iterations.max(1) {
        let start = Instant::now();
        operation(iteration)?;
        latencies.push(start.elapsed().as_micros().max(1) as u64);
    }
    Ok(())
}

fn operation_count_for(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> usize {
    match workload {
        RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => options
            .iterations_per_workload
            .saturating_mul(options.batch_size),
        _ => options.iterations_per_workload,
    }
}

fn writes_per_iteration(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> usize {
    match workload {
        RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => options.batch_size.max(1),
        _ => 1,
    }
}

fn benchmark_cluster() -> RaftCluster {
    RaftCluster::new(
        10,
        RaftConfig::default(),
        vec![benchmark_peer(1), benchmark_peer(2), benchmark_peer(3)],
    )
    .expect("benchmark cluster")
}

fn benchmark_peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 40_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn benchmark_wal_record(index: u64) -> RustRaftWalRecord {
    RustRaftWalRecord {
        group_id: 10,
        node_id: 1,
        hard_state: RustRaftHardState {
            current_term: 1,
            voted_for: Some(1),
            committed: Some(RustRaftLogId { term: 1, index }),
        },
        membership: RustRaftMembership {
            group_id: 10,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![RustRaftLogEntry {
            log_id: RustRaftLogId { term: 1, index },
            payload: b"wal-benchmark".to_vec(),
        }],
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

fn temp_benchmark_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-benchmark-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn git_revision_for(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|err| format!("git_revision_unavailable:{err}"))?;
    if !output.status.success() {
        return Err("git_revision_unavailable".to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn synthetic_latency_series(
    engine: RustRaftBenchmarkEngine,
    workload: RustRaftBenchmarkWorkload,
    iterations: usize,
) -> Vec<u64> {
    let base = match workload {
        RustRaftBenchmarkWorkload::SingleKeyWrites => 900,
        RustRaftBenchmarkWorkload::BatchedWrites => 1_600,
        RustRaftBenchmarkWorkload::ReplicationBatching => 1_250,
        RustRaftBenchmarkWorkload::WalFsync => 2_200,
        RustRaftBenchmarkWorkload::ReadIndexReads => 320,
        RustRaftBenchmarkWorkload::LeaseReads => 120,
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup => 8_000,
        RustRaftBenchmarkWorkload::SnapshotStreaming => 6_500,
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
    let batch_valid = !matches!(
        workload,
        RustRaftBenchmarkWorkload::BatchedWrites | RustRaftBenchmarkWorkload::ReplicationBatching
    ) || options.batch_size > 1;
    three_nodes && iterations_present && batch_valid
}
