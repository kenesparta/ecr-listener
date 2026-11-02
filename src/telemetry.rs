use std::error::Error;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// `aws_config` logs the credentials it resolved at INFO, and their `Debug` form carries the
/// access key id (the secret is redacted). Not a secret, but nothing the journal needs either,
/// so its target is held at WARN unless `RUST_LOG` names it explicitly.
const AWS_CONFIG_TARGET: &str = "aws_config";
const AWS_CONFIG_DIRECTIVE: &str = "aws_config=warn";

#[derive(Debug, thiserror::Error)]
#[error("cannot install the tracing subscriber")]
pub struct TelemetryError(#[source] Box<dyn Error + Send + Sync>);

/// Installs the one subscriber: plain lines on stderr, without colours or timestamps, since
/// journald stamps each line. `RUST_LOG` filters, defaulting to `info`; directives in it that
/// do not parse are reported on stderr and skipped. `aws_config` stays at `warn` unless
/// `RUST_LOG` mentions it.
pub fn init() -> Result<(), TelemetryError> {
    let rust_log = std::env::var(EnvFilter::DEFAULT_ENV).ok();
    tracing_subscriber::fmt()
        .with_env_filter(env_filter(rust_log.as_deref()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .without_time()
        .try_init()
        .map_err(TelemetryError)
}

fn env_filter(rust_log: Option<&str>) -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse_lossy(directives(rust_log))
}

/// The filter directives for a given `RUST_LOG`: the operator's own, plus the `aws_config`
/// clamp when they did not mention that target themselves.
fn directives(rust_log: Option<&str>) -> String {
    match rust_log.map(str::trim).filter(|spec| !spec.is_empty()) {
        None => format!("info,{AWS_CONFIG_DIRECTIVE}"),
        Some(spec) if spec.contains(AWS_CONFIG_TARGET) => spec.to_owned(),
        Some(spec) => format!("{spec},{AWS_CONFIG_DIRECTIVE}"),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[test]
    fn without_rust_log_the_default_is_info_with_aws_config_clamped() {
        assert_eq!(directives(None), "info,aws_config=warn");
        assert_eq!(directives(Some("  ")), "info,aws_config=warn");
    }

    #[test]
    fn an_operator_filter_gets_the_clamp_appended() {
        assert_eq!(directives(Some("debug")), "debug,aws_config=warn");
        assert_eq!(
            directives(Some("info,ecr_listener=debug")),
            "info,ecr_listener=debug,aws_config=warn"
        );
    }

    #[test]
    fn an_operator_filter_naming_aws_config_is_left_alone() {
        assert_eq!(
            directives(Some("info,aws_config=debug")),
            "info,aws_config=debug"
        );
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Capture {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emits what aws_config emits at startup, plus a line of our own, through the real filter.
    fn emitted_under(rust_log: Option<&str>) -> String {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter(rust_log))
            .with_writer(capture.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                target: "aws_config::profile::credentials",
                "loaded base credentials access_key_id=AKIAEXAMPLEEXAMPLE00"
            );
            tracing::info!(target: "ecr_listener::daemon", "listening");
        });
        capture.text()
    }

    #[test]
    fn the_credentials_line_is_dropped_by_default_and_ours_is_kept() {
        let text = emitted_under(None);
        assert!(!text.contains("AKIAEXAMPLEEXAMPLE00"), "{text}");
        assert!(text.contains("listening"), "{text}");
    }

    #[test]
    fn the_credentials_line_is_dropped_under_a_plain_info_filter() {
        // What the personal-infra unit set through ecr-listener v0.1.0.
        let text = emitted_under(Some("info"));
        assert!(!text.contains("AKIAEXAMPLEEXAMPLE00"), "{text}");
        assert!(text.contains("listening"), "{text}");
    }

    #[test]
    fn naming_aws_config_brings_the_credentials_line_back() {
        let text = emitted_under(Some("info,aws_config=info"));
        assert!(text.contains("AKIAEXAMPLEEXAMPLE00"), "{text}");
    }
}
