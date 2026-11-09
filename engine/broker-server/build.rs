use std::path::PathBuf;

fn main() {
    // All artifacts embed the same CI-verified committed assets. Rust builds
    // never run npm, access the network, or silently fall back after a UI error.
    let dist = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../panel/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    assert!(
        dist.join("index.html").is_file(),
        "engine/panel/dist/index.html missing — run `npm ci && npm run build` in engine/panel"
    );
}
