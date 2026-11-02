use std::error::Error;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, thiserror::Error)]
#[error("cannot install the tracing subscriber")]
pub struct TelemetryError(#[source] Box<dyn Error + Send + Sync>);

/// Installs the one subscriber: plain lines on stderr, without colours or timestamps, since
/// journald stamps each line. `RUST_LOG` filters, defaulting to `info`; directives in it that
/// do not parse are reported on stderr and skipped.
pub fn init() -> Result<(), TelemetryError> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .without_time()
        .try_init()
        .map_err(TelemetryError)
}
