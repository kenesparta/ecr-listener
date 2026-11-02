//! Startup, the poll loop and shutdown.

use std::error::Error;
use std::pin::pin;

use sd_notify::NotifyState;
use tokio::signal::unix::{SignalKind, signal};

use crate::config::Config;
use crate::executor::execute;
use crate::plan::plan_batch;
use crate::queue::{Queue, QueueError, SqsQueue};
use crate::retry::PollBackoff;
use crate::systemctl::{Systemctl, UnitStarter};

#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("cannot listen for SIGTERM and SIGINT")]
    Signals(#[source] std::io::Error),
    #[error("cannot read the queue's attributes; check the credentials and queue_url")]
    QueueCheck(#[source] QueueError),
}

/// Proves the credentials and the queue, tells systemd the service is ready, then polls until
/// SIGTERM or SIGINT. Returns an error only from startup; after `READY=1` every AWS failure is
/// logged and retried, never returned.
pub async fn run(config: Config) -> Result<(), StartupError> {
    let mut shutdown = pin!(shutdown_signal().map_err(StartupError::Signals)?);
    let queue = SqsQueue::connect(&config).await;
    let arn = tokio::select! {
        () = &mut shutdown => {
            notify(NotifyState::Stopping);
            return Ok(());
        }
        arn = queue.arn() => arn.map_err(StartupError::QueueCheck)?,
    };

    notify(NotifyState::Ready);
    tracing::info!(
        queue = %config.queue_url(),
        queue_arn = %arn,
        projects = config.projects().len(),
        "listening"
    );
    poll(&queue, &Systemctl::default(), &config, shutdown).await;
    notify(NotifyState::Stopping);
    tracing::info!("stopped");
    Ok(())
}

/// Receives, plans and executes batches until `shutdown` completes. Shutdown cancels a long
/// poll or a running batch on the spot: whatever was not deleted is delivered again, and a
/// repeated deploy is idempotent.
pub(crate) async fn poll<Q, S>(
    queue: &Q,
    starter: &S,
    config: &Config,
    shutdown: impl Future<Output = ()>,
) where
    Q: Queue,
    S: UnitStarter,
{
    let mut shutdown = pin!(shutdown);
    let mut backoff = PollBackoff::default();
    loop {
        let received = tokio::select! {
            () = &mut shutdown => return,
            received = queue.receive() => received,
        };
        let messages = match received {
            Ok(messages) => {
                backoff.reset();
                messages
            }
            Err(error) => {
                let delay = backoff.next_delay();
                tracing::error!(
                    error = &error as &dyn Error,
                    retry_in_secs = delay.as_secs(),
                    "cannot receive messages"
                );
                tokio::select! {
                    () = &mut shutdown => return,
                    () = tokio::time::sleep(delay) => continue,
                }
            }
        };
        if messages.is_empty() {
            continue;
        }
        tracing::debug!(messages = messages.len(), "received");
        let plan = plan_batch(&messages, config);
        tokio::select! {
            () = &mut shutdown => return,
            () = execute(queue, starter, config.deploy_timeout(), plan) => {}
        }
    }
}

/// Registers the handlers now, so a signal that arrives during startup is not lost.
fn shutdown_signal() -> std::io::Result<impl Future<Output = ()>> {
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        let name = tokio::select! {
            _ = terminate.recv() => "SIGTERM",
            _ = interrupt.recv() => "SIGINT",
        };
        tracing::info!(signal = name, "shutting down");
    })
}

/// A no-op when `NOTIFY_SOCKET` is unset, i.e. when not run by systemd as `Type=notify`.
fn notify(state: NotifyState<'_>) {
    if let Err(error) = sd_notify::notify(std::slice::from_ref(&state)) {
        tracing::error!(
            state = %state,
            error = &error as &dyn Error,
            "cannot notify systemd"
        );
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, Instant};

    use super::*;
    use crate::testing::{
        BLOG_UNIT, Call, FakeQueue, FakeStarter, Journal, Outcome, config, message, push_body,
        queue_error,
    };

    fn blog_push(id: &str) -> crate::message::Received {
        message(
            id,
            push_body("kenespartadev", Some("latest"), "sha256:a"),
            1,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn receive_errors_back_off_and_a_success_resets_the_backoff() {
        let journal = Journal::default();
        let queue = FakeQueue::with_inbox(
            &journal,
            [
                Err(queue_error()),
                Err(queue_error()),
                Ok(vec![blog_push("m1")]),
                Err(queue_error()),
            ],
        );
        let starter = FakeStarter::new(&journal);
        let drained = queue.drained.clone();
        let started = Instant::now();

        poll(&queue, &starter, &config(), async move {
            drained.notified().await
        })
        .await;

        // 1 s and 2 s after the first two errors, then 1 s again: the success reset it.
        assert_eq!(started.elapsed(), Duration::from_secs(4));
        assert_eq!(
            journal.calls(),
            [
                Call::ChangeVisibility(vec![("handle-m1".to_owned(), 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                Call::Delete(vec!["handle-m1".to_owned()]),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_receives_poll_again_without_delay() {
        let journal = Journal::default();
        let queue = FakeQueue::with_inbox(&journal, [Ok(vec![]), Ok(vec![]), Ok(vec![])]);
        let starter = FakeStarter::new(&journal);
        let drained = queue.drained.clone();
        let started = Instant::now();

        poll(&queue, &starter, &config(), async move {
            drained.notified().await
        })
        .await;

        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(journal.calls(), []);
    }

    #[tokio::test]
    async fn shutdown_abandons_a_running_deploy_without_deleting_its_messages() {
        let journal = Journal::default();
        let queue = FakeQueue::with_inbox(&journal, [Ok(vec![blog_push("m1")])]);
        let starter = FakeStarter::new(&journal).with(BLOG_UNIT, Outcome::Hang);
        let hung = starter.hung.clone();

        poll(
            &queue,
            &starter,
            &config(),
            async move { hung.notified().await },
        )
        .await;

        assert_eq!(
            journal.calls(),
            [
                Call::ChangeVisibility(vec![("handle-m1".to_owned(), 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
            ]
        );
    }
}
