//! Carries out a `Plan`: deletes what is settled, defers what is not, and runs the deploys one
//! at a time. A message is deleted only after the deploy it asked for succeeded; every failure
//! here leaves messages on the queue, where SQS delivers them again.

use std::error::Error;

use tracing::Instrument;

use crate::config::DeployTimeout;
use crate::message::Receipt;
use crate::plan::{Deploy, Plan};
use crate::queue::{EntryFailure, MAX_BATCH, Queue, VisibilityChange};
use crate::retry::VisibilityTimeout;
use crate::systemctl::UnitStarter;

pub(crate) async fn execute<Q, S>(queue: &Q, starter: &S, timeout: DeployTimeout, plan: Plan<'_>)
where
    Q: Queue,
    S: UnitStarter,
{
    let Plan {
        deploys,
        ignore,
        retain,
    } = plan;

    for ignored in &ignore {
        tracing::info!(
            message_id = %ignored.receipt.message_id,
            repository = %ignored.action.repository,
            tag = ignored.action.tag.as_deref(),
            action_type = %ignored.action.action_type,
            result = %ignored.action.result,
            reason = %ignored.reason,
            "ignored"
        );
    }
    let ignored: Vec<Receipt> = ignore.into_iter().map(|ignored| ignored.receipt).collect();
    delete(queue, &ignored).await;

    for retained in &retain {
        tracing::warn!(
            message_id = %retained.receipt.message_id,
            receive_count = retained.receipt.receive_count.get(),
            error = &retained.error as &dyn Error,
            "unexpected message; leaving it on the queue, from where it moves to the DLQ"
        );
    }
    let retained: Vec<&Receipt> = retain.iter().map(|retained| &retained.receipt).collect();
    retry_later(queue, &retained).await;

    // One after another: concurrent pulls would compete for the host's memory.
    for deploy in deploys {
        let span = tracing::info_span!("deploy", project = %deploy.project.name());
        run_deploy(queue, starter, timeout, deploy)
            .instrument(span)
            .await;
    }
}

async fn run_deploy<Q, S>(queue: &Q, starter: &S, timeout: DeployTimeout, deploy: Deploy<'_>)
where
    Q: Queue,
    S: UnitStarter,
{
    let unit = deploy.project.unit();
    let hold = VisibilityTimeout::hold_for(timeout);
    let held: Vec<VisibilityChange<'_>> = deploy
        .receipts
        .iter()
        .map(|receipt| VisibilityChange {
            receipt,
            timeout: hold,
        })
        .collect();
    // If the hold fails the deploy still runs: at worst a message reappears early and causes
    // a second, idempotent deploy.
    change_visibility(queue, &held).await;

    tracing::info!(
        %unit,
        digests = %deploy.digests.join(" "),
        messages = deploy.receipts.len(),
        "deploying"
    );
    match starter.start(unit, timeout.as_duration()).await {
        Ok(()) => {
            tracing::info!(%unit, "deployed");
            delete(queue, &deploy.receipts).await;
        }
        Err(error) => {
            tracing::error!(
                %unit,
                error = &error as &dyn Error,
                systemctl_stderr = error.stderr(),
                hint = %format!("journalctl -u {unit}"),
                "deploy failed; its messages will be delivered again"
            );
            let receipts: Vec<&Receipt> = deploy.receipts.iter().collect();
            retry_later(queue, &receipts).await;
        }
    }
}

/// Hides each message for its retry delay, which grows with its receive count.
async fn retry_later<Q: Queue>(queue: &Q, receipts: &[&Receipt]) {
    let changes: Vec<VisibilityChange<'_>> = receipts
        .iter()
        .map(|&receipt| VisibilityChange {
            receipt,
            timeout: VisibilityTimeout::for_retry(receipt.receive_count),
        })
        .collect();
    for change in &changes {
        tracing::info!(
            message_id = %change.receipt.message_id,
            receive_count = change.receipt.receive_count.get(),
            retry_in_secs = change.timeout.as_secs(),
            "deferred"
        );
    }
    change_visibility(queue, &changes).await;
}

async fn delete<Q: Queue>(queue: &Q, receipts: &[Receipt]) {
    for chunk in receipts.chunks(MAX_BATCH) {
        match queue.delete(chunk).await {
            Ok(failures) => log_entry_failures(&failures, "cannot delete a message"),
            Err(error) => tracing::warn!(
                messages = chunk.len(),
                error = &error as &dyn Error,
                "cannot delete messages; they will be delivered again"
            ),
        }
    }
}

async fn change_visibility<Q: Queue>(queue: &Q, changes: &[VisibilityChange<'_>]) {
    for chunk in changes.chunks(MAX_BATCH) {
        match queue.change_visibility(chunk).await {
            Ok(failures) => log_entry_failures(&failures, "cannot change a message's visibility"),
            Err(error) => tracing::warn!(
                messages = chunk.len(),
                error = &error as &dyn Error,
                "cannot change the visibility of messages; they keep their current timeout"
            ),
        }
    }
}

fn log_entry_failures(failures: &[EntryFailure], what: &str) {
    for failure in failures {
        tracing::warn!(
            message_id = failure.message_id.as_ref().map(|id| id.as_str()),
            code = %failure.code,
            detail = failure.message.as_deref(),
            sender_fault = failure.sender_fault,
            "{what}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Received;
    use crate::plan::plan_batch;
    use crate::testing::{
        API_UNIT, BLOG_UNIT, Call, FakeQueue, FakeStarter, Journal, Outcome, config, message,
        push_body,
    };

    async fn run(queue: &FakeQueue, starter: &FakeStarter, messages: &[Received]) {
        let config = config();
        let plan = plan_batch(messages, &config);
        execute(queue, starter, config.deploy_timeout(), plan).await;
    }

    fn blog_push(id: &str, receive_count: u32) -> Received {
        message(
            id,
            push_body("kenespartadev", Some("latest"), "sha256:a"),
            receive_count,
        )
    }

    fn api_push(id: &str, receive_count: u32) -> Received {
        message(
            id,
            push_body("budget-api", Some("latest"), "sha256:c"),
            receive_count,
        )
    }

    fn handles(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| format!("handle-{id}")).collect()
    }

    fn visibility(changes: &[(&str, i32)]) -> Call {
        Call::ChangeVisibility(
            changes
                .iter()
                .map(|(id, secs)| (format!("handle-{id}"), *secs))
                .collect(),
        )
    }

    // The test config's deploy timeout is 300 s, so a hold is 360 s.

    #[tokio::test]
    async fn a_successful_deploy_holds_starts_then_deletes() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal);
        run(&queue, &starter, &[blog_push("m1", 1)]).await;
        assert_eq!(
            journal.calls(),
            [
                visibility(&[("m1", 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                Call::Delete(handles(&["m1"])),
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_deploy_keeps_its_messages_and_backs_off_by_receive_count() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal).with(BLOG_UNIT, Outcome::Fail);
        run(&queue, &starter, &[blog_push("m1", 1), blog_push("m2", 3)]).await;
        assert_eq!(
            journal.calls(),
            [
                visibility(&[("m1", 360), ("m2", 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                visibility(&[("m1", 60), ("m2", 240)]),
            ]
        );
    }

    #[tokio::test]
    async fn a_timed_out_deploy_is_retried_like_a_failure() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal).with(BLOG_UNIT, Outcome::TimeOut);
        run(&queue, &starter, &[blog_push("m1", 8)]).await;
        assert_eq!(
            journal.calls(),
            [
                visibility(&[("m1", 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                visibility(&[("m1", 900)]),
            ]
        );
    }

    #[tokio::test]
    async fn projects_deploy_one_at_a_time_in_config_order() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal);
        run(&queue, &starter, &[api_push("a1", 1), blog_push("b1", 1)]).await;
        assert_eq!(
            journal.calls(),
            [
                visibility(&[("b1", 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                Call::Delete(handles(&["b1"])),
                visibility(&[("a1", 360)]),
                Call::Start(API_UNIT.to_owned()),
                Call::Delete(handles(&["a1"])),
            ]
        );
    }

    #[tokio::test]
    async fn a_failing_project_does_not_stop_the_next_one() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal).with(BLOG_UNIT, Outcome::Fail);
        run(&queue, &starter, &[blog_push("b1", 2), api_push("a1", 1)]).await;
        assert_eq!(
            journal.calls(),
            [
                visibility(&[("b1", 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                visibility(&[("b1", 120)]),
                visibility(&[("a1", 360)]),
                Call::Start(API_UNIT.to_owned()),
                Call::Delete(handles(&["a1"])),
            ]
        );
    }

    #[tokio::test]
    async fn ignored_messages_are_deleted_and_unexpected_ones_deferred() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal);
        let messages = [
            message("untagged", push_body("kenespartadev", None, "sha256:a"), 1),
            message("garbage", "<html>".to_owned(), 2),
        ];
        run(&queue, &starter, &messages).await;
        assert_eq!(
            journal.calls(),
            [
                Call::Delete(handles(&["untagged"])),
                visibility(&[("garbage", 120)])
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_hold_does_not_block_the_deploy_or_its_delete() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal).failing_visibility_changes();
        let starter = FakeStarter::new(&journal);
        run(&queue, &starter, &[blog_push("m1", 1)]).await;
        assert_eq!(
            journal.calls(),
            [
                visibility(&[("m1", 360)]),
                Call::Start(BLOG_UNIT.to_owned()),
                Call::Delete(handles(&["m1"])),
            ]
        );
    }

    #[tokio::test]
    async fn deletes_are_sent_in_batches_of_at_most_ten() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal);
        let ids: Vec<String> = (0..12).map(|n| format!("u{n}")).collect();
        let messages: Vec<_> = ids
            .iter()
            .map(|id| message(id, push_body("kenespartadev", None, "sha256:a"), 1))
            .collect();
        run(&queue, &starter, &messages).await;
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        assert_eq!(
            journal.calls(),
            [
                Call::Delete(handles(&ids[..10])),
                Call::Delete(handles(&ids[10..])),
            ]
        );
    }

    #[tokio::test]
    async fn an_empty_plan_calls_nothing() {
        let journal = Journal::default();
        let queue = FakeQueue::new(&journal);
        let starter = FakeStarter::new(&journal);
        run(&queue, &starter, &[]).await;
        assert_eq!(journal.calls(), []);
    }
}
