use rustraft::benchmark::{
    rustraft_assert_production_byteraft_parity, rustraft_find_byteraft_harness,
    rustraft_run_byteraft_parity_benchmark, RustRaftBenchmarkOptions,
    RustRaftExternalByteRaftRunner, RustRaftRuntimeBenchmarkRunner,
};
use std::path::PathBuf;

fn main() {
    let options = RustRaftBenchmarkOptions::default();
    let build_profile =
        std::env::var("RUSTRAFT_BENCHMARK_PROFILE").unwrap_or_else(|_| "debug".to_string());
    let byteraft_root = std::env::var("BYTERAFT_ROOT").ok().map(PathBuf::from);
    let byteraft_bin = std::env::var("BYTERAFT_BENCHMARK_BIN")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            byteraft_root
                .as_ref()
                .and_then(|root| rustraft_find_byteraft_harness(root).ok())
        });
    let Some(byteraft_bin) = byteraft_bin else {
        eprintln!(
            "ByteRaft parity benchmark failed: benchmark:real_byteraft_missing; set BYTERAFT_ROOT or BYTERAFT_BENCHMARK_BIN"
        );
        std::process::exit(2);
    };
    let mut byteraft =
        match RustRaftExternalByteRaftRunner::new(byteraft_bin, byteraft_root, &build_profile) {
            Ok(runner) => runner,
            Err(error) => {
                eprintln!("ByteRaft parity benchmark failed: {error}");
                std::process::exit(2);
            }
        };
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new(build_profile);
    let report = rustraft_run_byteraft_parity_benchmark(&mut byteraft, &mut rustraft, &options);

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Err(blockers) = rustraft_assert_production_byteraft_parity(&report) {
        eprintln!("ByteRaft parity benchmark failed: {blockers}");
        std::process::exit(1);
    }
}
