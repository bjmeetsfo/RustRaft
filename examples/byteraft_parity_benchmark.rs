use rustraft::benchmark::{
    rustraft_assert_byteraft_parity, rustraft_run_byteraft_parity_benchmark,
    RustRaftBenchmarkOptions, RustRaftSameMachineModelRunner,
};

fn main() {
    let options = RustRaftBenchmarkOptions::default();
    let mut byteraft = RustRaftSameMachineModelRunner::byteraft_baseline();
    let mut rustraft = RustRaftSameMachineModelRunner::rustraft_candidate();
    let report = rustraft_run_byteraft_parity_benchmark(&mut byteraft, &mut rustraft, &options);

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Err(blockers) = rustraft_assert_byteraft_parity(&report) {
        eprintln!("ByteRaft parity benchmark failed: {blockers}");
        std::process::exit(1);
    }
}
