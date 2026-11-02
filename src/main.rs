use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;
use ecr_listener::cli::Cli;
use ecr_listener::config::Config;
use ecr_listener::{daemon, telemetry};

/// The exit status of an unusable configuration; a failed start exits with 1.
const EXIT_INVALID_CONFIG: u8 = 2;

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = telemetry::init() {
        eprintln!("ecr-listener: {error}");
        return ExitCode::FAILURE;
    }
    // Validated before the runtime and the AWS client exist.
    let config = match Config::load(&cli.config) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("{:#}", anyhow::Error::new(error));
            return ExitCode::from(EXIT_INVALID_CONFIG);
        }
    };
    match serve(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

fn serve(config: Config) -> anyhow::Result<()> {
    // One thread is plenty: the daemon waits on one long poll or one child process at a time.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("cannot start the Tokio runtime")?;
    runtime.block_on(daemon::run(config))?;
    Ok(())
}
