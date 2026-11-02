//! Starting a deploy unit. `systemctl start` on a `Type=oneshot` unit blocks until the unit's
//! process exits, so its exit status is the deploy's outcome.

use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::process::Command;

use crate::config::UnitName;

#[derive(Debug, thiserror::Error)]
pub(crate) enum StartError {
    #[error("cannot run systemctl")]
    Spawn(#[source] std::io::Error),
    #[error("cannot wait for systemctl")]
    Wait(#[source] std::io::Error),
    #[error("systemctl start failed ({status})")]
    Failed { status: ExitStatus, stderr: String },
    #[error("systemctl start did not finish within {} s", .timeout.as_secs())]
    TimedOut { timeout: Duration },
}

impl StartError {
    /// What systemctl wrote to stderr before failing, when it wrote anything.
    pub(crate) fn stderr(&self) -> Option<&str> {
        match self {
            Self::Failed { stderr, .. } if !stderr.is_empty() => Some(stderr),
            _ => None,
        }
    }
}

pub(crate) trait UnitStarter {
    /// Starts `unit` and waits for it, for at most `timeout`.
    fn start(
        &self,
        unit: &UnitName,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), StartError>> + Send;
}

/// Runs `systemctl start --no-ask-password -- <unit>`. The `ecr-listener` user may do that only
/// for the deploy units, by a polkit rule managed outside this program.
pub(crate) struct Systemctl {
    program: &'static str,
    /// Words placed before `start`; empty outside tests, where they turn `/bin/sh` into a
    /// stand-in for systemctl.
    leading_args: Vec<&'static str>,
}

impl Default for Systemctl {
    fn default() -> Self {
        Self {
            program: "systemctl",
            leading_args: Vec::new(),
        }
    }
}

impl UnitStarter for Systemctl {
    async fn start(&self, unit: &UnitName, timeout: Duration) -> Result<(), StartError> {
        let child = Command::new(self.program)
            .args(&self.leading_args)
            .args(["start", "--no-ask-password", "--", unit.as_str()])
            // The notify socket is this daemon's; systemctl has no business with it.
            .env_remove("NOTIFY_SOCKET")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            // On a timeout or shutdown the client is killed. The systemd job it queued keeps
            // running, and a later `systemctl start` waits for that same job.
            .kill_on_drop(true)
            .spawn()
            .map_err(StartError::Spawn)?;
        let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(output) => output.map_err(StartError::Wait)?,
            // `Elapsed` says nothing the timeout itself does not.
            Err(_elapsed) => return Err(StartError::TimedOut { timeout }),
        };
        if output.status.success() {
            Ok(())
        } else {
            Err(StartError::Failed {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    fn unit() -> UnitName {
        UnitName::try_from("personal-infra-deploy@blog.service".to_owned()).unwrap()
    }

    /// `/bin/sh -c <script> systemctl …`: the script sees systemctl's arguments as `$@`.
    fn shell(script: &'static str) -> Systemctl {
        Systemctl {
            program: "/bin/sh",
            leading_args: vec!["-c", script, "systemctl"],
        }
    }

    #[tokio::test]
    async fn exit_status_zero_is_success() {
        let result = shell("exit 0")
            .start(&unit(), Duration::from_secs(10))
            .await;
        assert!(matches!(result, Ok(())), "got {result:?}");
    }

    #[tokio::test]
    async fn the_unit_is_passed_after_the_option_terminator() {
        let script = r#"printf '%s\n' "$@" >&2; exit 3"#;
        let result = shell(script).start(&unit(), Duration::from_secs(10)).await;
        let Err(error @ StartError::Failed { status, .. }) = &result else {
            panic!("expected a failure, got {result:?}");
        };
        assert_eq!(status.code(), Some(3));
        assert_eq!(
            error.stderr(),
            Some("start\n--no-ask-password\n--\npersonal-infra-deploy@blog.service")
        );
    }

    #[tokio::test]
    async fn a_failure_without_output_has_no_stderr() {
        let result = shell("exit 1")
            .start(&unit(), Duration::from_secs(10))
            .await;
        let Err(error) = &result else {
            panic!("expected a failure, got {result:?}");
        };
        assert!(matches!(error, StartError::Failed { .. }), "got {error:?}");
        assert_eq!(error.stderr(), None);
    }

    #[tokio::test]
    async fn a_unit_that_outlives_the_timeout_times_out_promptly() {
        let started = Instant::now();
        let result = shell("exec sleep 30")
            .start(&unit(), Duration::from_millis(200))
            .await;
        assert!(
            matches!(result, Err(StartError::TimedOut { timeout }) if timeout == Duration::from_millis(200)),
            "got {result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn a_missing_program_is_a_spawn_error() {
        let starter = Systemctl {
            program: "/nonexistent/systemctl",
            leading_args: Vec::new(),
        };
        let result = starter.start(&unit(), Duration::from_secs(10)).await;
        assert!(
            matches!(result, Err(StartError::Spawn(_))),
            "got {result:?}"
        );
    }
}
