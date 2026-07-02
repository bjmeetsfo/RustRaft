use rustraft::readiness::rustraft_open_source_surface;

fn main() {
    let surface = rustraft_open_source_surface();
    println!("{}", serde_json::to_string_pretty(&surface).unwrap());
}
