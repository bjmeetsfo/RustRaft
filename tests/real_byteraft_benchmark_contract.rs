use rustraft::benchmark::{
    rustraft_assert_production_byteraft_parity, rustraft_byteraft_benchmark_evidence,
    rustraft_find_byteraft_harness, rustraft_find_or_build_byteraft_harness,
    rustraft_run_byteraft_parity_benchmark, RustRaftBenchmarkEngineSource,
    RustRaftBenchmarkOptions, RustRaftExternalByteRaftRunner, RustRaftRuntimeBenchmarkRunner,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-real-byteraft-{name}-{}-{nonce}",
        std::process::id()
    ))
}

#[cfg(unix)]
fn make_fake_byteraft_harness(root: &std::path::Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let bin = bin_dir.join("byteraft_parity_benchmark");
    fs::write(
        &bin,
        r#"#!/usr/bin/env bash
set -euo pipefail
workload="single_key_writes"
node_count=3
iterations=4
while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload) workload="$2"; shift 2 ;;
    --node-count) node_count="$2"; shift 2 ;;
    --iterations) iterations="$2"; shift 2 ;;
    --batch-size) shift 2 ;;
    --byteraft-root) shift 2 ;;
    *) shift ;;
  esac
done
cat <<JSON
{
  "workload": "$workload",
  "engine": "byte_raft",
  "engine_source": "real_byte_raft",
  "binary_path": null,
  "git_revision": null,
  "build_profile": "fake-test",
  "node_count": $node_count,
  "operation_count": $iterations,
  "p50_latency_micros": 1000000,
  "p99_latency_micros": 1000000,
  "throughput_ops_per_sec": 1.0,
  "correctness_passed": true
}
JSON
"#,
    )
    .expect("write fake harness");
    let mut perms = fs::metadata(&bin).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&bin, perms).expect("chmod");
    bin
}

#[cfg(unix)]
#[test]
fn real_byteraft_runner_uses_external_harness_and_production_sources() {
    let root = temp_dir("root");
    let fake_bin = make_fake_byteraft_harness(&root);
    assert_eq!(
        rustraft_find_byteraft_harness(&root).expect("find"),
        fake_bin
    );

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: 2,
        batch_size: 2,
        ..Default::default()
    };
    let mut byteraft =
        RustRaftExternalByteRaftRunner::from_root(&root, "fake-test").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("test");
    let report = rustraft_run_byteraft_parity_benchmark(&mut byteraft, &mut rustraft, &options);
    rustraft_assert_production_byteraft_parity(&report).expect("production parity");

    let evidence = rustraft_byteraft_benchmark_evidence(&report);
    assert!(evidence.real_byteraft);
    assert!(evidence.rustraft_runtime);
    assert!(evidence.correctness_passed);
    assert!(evidence.performance_within_threshold);
    assert!(evidence.blockers.is_empty());
    assert!(report.comparisons.iter().all(|comparison| {
        comparison.byteraft.engine_source == RustRaftBenchmarkEngineSource::RealByteRaft
            && comparison.rustraft.engine_source == RustRaftBenchmarkEngineSource::RustRaftRuntime
    }));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn byteraft_runner_builds_harness_from_checkout_hook_before_failing_closed() {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_dir("build-hook");
    let scripts = root.join("scripts");
    fs::create_dir_all(&scripts).expect("scripts");
    let build_script = scripts.join("build_byteraft_parity_benchmark.sh");
    fs::write(
        &build_script,
        r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p bin
cat > bin/byteraft_parity_benchmark <<'HARNESS'
#!/usr/bin/env bash
cat <<JSON
{
  "workload": "single_key_writes",
  "engine": "byte_raft",
  "engine_source": "real_byte_raft",
  "binary_path": null,
  "git_revision": null,
  "build_profile": "fake-build-hook",
  "node_count": 3,
  "operation_count": 1,
  "p50_latency_micros": 1000000,
  "p99_latency_micros": 1000000,
  "throughput_ops_per_sec": 1.0,
  "correctness_passed": true
}
JSON
HARNESS
chmod +x bin/byteraft_parity_benchmark
"#,
    )
    .expect("build script");
    let mut perms = fs::metadata(&build_script).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&build_script, perms).expect("chmod");

    let built =
        rustraft_find_or_build_byteraft_harness(&root, "debug").expect("build fake harness");

    assert_eq!(built, root.join("bin/byteraft_parity_benchmark"));
    assert!(built.is_file());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn missing_byteraft_harness_fails_closed_with_real_byteraft_blocker() {
    let root = temp_dir("missing");
    fs::create_dir_all(&root).expect("root");
    let err = rustraft_find_byteraft_harness(&root).expect_err("missing harness");
    assert!(err.contains("benchmark:real_byteraft_missing"));
    let err =
        rustraft_find_or_build_byteraft_harness(&root, "debug").expect_err("missing build target");
    assert!(err.contains("benchmark:real_byteraft_missing"));
    let _ = fs::remove_dir_all(root);
}
