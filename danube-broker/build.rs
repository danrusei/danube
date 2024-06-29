use std::error::Error;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let out_dir = Path::new("src/proto");
    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["../proto/DanubeApi.proto"], &["../proto"])?;

    let client_proto_dir = Path::new("../danube-client/src/proto");

    // copy the generated file to danube-client crate
    fs::copy(
        out_dir.join("danube.rs"),
        client_proto_dir.join("danube.rs"),
    )?;

    Ok(())
}
