mod prepare_upload;
mod transcode;

use std::path::Path;
use std::process::ExitCode;

use clap::Parser;
use prepare_upload::PrepareUploadArgs;

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("prepare-upload") => {
            let mut clap_args = vec!["prepare-upload".to_string()];
            clap_args.extend(std::env::args().skip(2));
            let parsed = PrepareUploadArgs::try_parse_from(clap_args)?;
            prepare_upload::run(parsed)
        }
        Some(dir) => transcode::run(Path::new(dir)),
        None => Err(
            "Usage:\n  parquet-transcode <directory>\n  parquet-transcode prepare-upload --input ... --output ... --dataset-id ... --signal-id ..."
                .into(),
        ),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
