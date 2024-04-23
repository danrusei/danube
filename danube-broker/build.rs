use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        .out_dir("../proto")
        .compile(&["../proto/DanubeApi.proto"], &["../proto"])?;
    Ok(())
}
