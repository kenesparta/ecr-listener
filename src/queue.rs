//! The queue: the `Queue` port the executor and the poll loop run against, and its
//! `aws-sdk-sqs` adapter. The adapter makes outbound HTTPS calls only.

use std::error::Error;
use std::num::NonZeroU32;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequestEntry,
    DeleteMessageBatchRequestEntry, Message, MessageSystemAttributeName, QueueAttributeName,
};

use crate::config::{Config, QueueUrl};
use crate::message::{MessageId, Receipt, ReceiptHandle, Received};
use crate::retry::VisibilityTimeout;

/// SQS's limit for one `ReceiveMessage` and for the entries of one batch call.
pub(crate) const MAX_BATCH: usize = 10;
const MAX_MESSAGES: i32 = 10;
const WAIT_TIME_SECS: i32 = 20;
/// Caps each SDK call, retries included, so a stalled connection cannot hang the 20 s long
/// poll (or any other call) forever.
const SDK_TIMEOUT: Duration = Duration::from_secs(40);

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("{operation} failed")]
    Request {
        operation: &'static str,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("GetQueueAttributes returned no QueueArn")]
    MissingQueueArn,
}

/// A batch entry SQS refused while accepting the call.
#[derive(Debug)]
pub(crate) struct EntryFailure {
    /// `None` only if SQS answered with an entry ID the request never sent.
    pub(crate) message_id: Option<MessageId>,
    pub(crate) code: String,
    pub(crate) message: Option<String>,
    pub(crate) sender_fault: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VisibilityChange<'a> {
    pub(crate) receipt: &'a Receipt,
    pub(crate) timeout: VisibilityTimeout,
}

pub(crate) trait Queue {
    /// One long poll for up to `MAX_BATCH` messages.
    fn receive(&self) -> impl Future<Output = Result<Vec<Received>, QueueError>> + Send;

    /// Deletes at most `MAX_BATCH` messages, returning the entries SQS refused.
    fn delete(
        &self,
        receipts: &[Receipt],
    ) -> impl Future<Output = Result<Vec<EntryFailure>, QueueError>> + Send;

    /// Changes the visibility of at most `MAX_BATCH` messages, returning the entries SQS
    /// refused.
    fn change_visibility(
        &self,
        changes: &[VisibilityChange<'_>],
    ) -> impl Future<Output = Result<Vec<EntryFailure>, QueueError>> + Send;
}

pub(crate) struct SqsQueue {
    client: Client,
    url: QueueUrl,
}

impl SqsQueue {
    /// Builds the client: the default credential chain, the region from the file (never from
    /// the environment) and `SDK_TIMEOUT` on every call. Makes no network call.
    pub(crate) async fn connect(config: &Config) -> Self {
        let timeouts = TimeoutConfig::builder()
            .operation_timeout(SDK_TIMEOUT)
            .operation_attempt_timeout(SDK_TIMEOUT)
            .build();
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region().as_str().to_owned()))
            .timeout_config(timeouts)
            .load()
            .await;
        Self {
            client: Client::new(&sdk),
            url: config.queue_url().clone(),
        }
    }

    /// Reads the queue's ARN, which proves both the credentials and the queue URL.
    pub(crate) async fn arn(&self) -> Result<String, QueueError> {
        let output = self
            .client
            .get_queue_attributes()
            .queue_url(self.url.as_str())
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(request_failed("GetQueueAttributes"))?;
        output
            .attributes()
            .and_then(|attributes| attributes.get(&QueueAttributeName::QueueArn))
            .cloned()
            .ok_or(QueueError::MissingQueueArn)
    }
}

impl Queue for SqsQueue {
    async fn receive(&self) -> Result<Vec<Received>, QueueError> {
        let output = self
            .client
            .receive_message()
            .queue_url(self.url.as_str())
            .max_number_of_messages(MAX_MESSAGES)
            .wait_time_seconds(WAIT_TIME_SECS)
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .send()
            .await
            .map_err(request_failed("ReceiveMessage"))?;
        let mut received = Vec::new();
        for message in output.messages.unwrap_or_default() {
            match into_received(message) {
                Some(message) => received.push(message),
                // SQS always sends both; without them the message can be neither deleted nor
                // deferred, so it simply reappears after the queue's visibility timeout.
                None => tracing::warn!("skipping a message without a message ID or receipt handle"),
            }
        }
        Ok(received)
    }

    async fn delete(&self, receipts: &[Receipt]) -> Result<Vec<EntryFailure>, QueueError> {
        let entries = receipts
            .iter()
            .enumerate()
            .map(|(index, receipt)| {
                DeleteMessageBatchRequestEntry::builder()
                    .id(index.to_string())
                    .receipt_handle(receipt.handle.as_str())
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(request_failed("DeleteMessageBatch"))?;
        let output = self
            .client
            .delete_message_batch()
            .queue_url(self.url.as_str())
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(request_failed("DeleteMessageBatch"))?;
        Ok(entry_failures(output.failed(), receipts.iter()))
    }

    async fn change_visibility(
        &self,
        changes: &[VisibilityChange<'_>],
    ) -> Result<Vec<EntryFailure>, QueueError> {
        let entries = changes
            .iter()
            .enumerate()
            .map(|(index, change)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(index.to_string())
                    .receipt_handle(change.receipt.handle.as_str())
                    .visibility_timeout(change.timeout.as_secs())
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(request_failed("ChangeMessageVisibilityBatch"))?;
        let output = self
            .client
            .change_message_visibility_batch()
            .queue_url(self.url.as_str())
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(request_failed("ChangeMessageVisibilityBatch"))?;
        Ok(entry_failures(
            output.failed(),
            changes.iter().map(|change| change.receipt),
        ))
    }
}

fn request_failed<E>(operation: &'static str) -> impl FnOnce(E) -> QueueError
where
    E: Error + Send + Sync + 'static,
{
    move |source| QueueError::Request {
        operation,
        source: Box::new(source),
    }
}

fn into_received(message: Message) -> Option<Received> {
    // A missing or unreadable count is treated as a first delivery: the shortest retry delay.
    let receive_count = message
        .attributes
        .as_ref()
        .and_then(|attributes| attributes.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .and_then(|raw| raw.parse::<NonZeroU32>().ok())
        .unwrap_or(NonZeroU32::MIN);
    let receipt = Receipt {
        message_id: MessageId::new(message.message_id?),
        handle: ReceiptHandle::new(message.receipt_handle?),
        receive_count,
    };
    // A message without a body is as unparseable as an empty one, and is kept the same way.
    let body = message.body.unwrap_or_default();
    Some(Received { receipt, body })
}

/// Maps the batch entry IDs (each request entry's position) back to message IDs.
fn entry_failures<'a>(
    failed: &[BatchResultErrorEntry],
    sent: impl Iterator<Item = &'a Receipt> + Clone,
) -> Vec<EntryFailure> {
    failed
        .iter()
        .map(|entry| EntryFailure {
            message_id: entry
                .id()
                .parse::<usize>()
                .ok()
                .and_then(|index| sent.clone().nth(index))
                .map(|receipt| receipt.message_id.clone()),
            code: entry.code().to_owned(),
            message: entry.message().map(str::to_owned),
            sender_fault: entry.sender_fault(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn sdk_message(attributes: Option<HashMap<MessageSystemAttributeName, String>>) -> Message {
        Message::builder()
            .message_id("m1")
            .receipt_handle("h1")
            .body("{}")
            .set_attributes(attributes)
            .build()
    }

    fn count(raw: &str) -> Option<HashMap<MessageSystemAttributeName, String>> {
        Some(HashMap::from([(
            MessageSystemAttributeName::ApproximateReceiveCount,
            raw.to_owned(),
        )]))
    }

    #[test]
    fn the_receive_count_comes_from_the_system_attribute() {
        let received = into_received(sdk_message(count("4"))).unwrap();
        assert_eq!(received.receipt.receive_count.get(), 4);
        assert_eq!(received.receipt.message_id.as_str(), "m1");
        assert_eq!(received.receipt.handle.as_str(), "h1");
        assert_eq!(received.body, "{}");
    }

    #[test]
    fn a_missing_or_unusable_receive_count_counts_as_the_first_delivery() {
        for attributes in [None, count("0"), count("-3"), count("many")] {
            let received = into_received(sdk_message(attributes.clone())).unwrap();
            assert_eq!(
                received.receipt.receive_count,
                NonZeroU32::MIN,
                "{attributes:?}"
            );
        }
    }

    #[test]
    fn a_message_without_a_receipt_handle_is_skipped() {
        let message = Message::builder().message_id("m1").body("{}").build();
        assert!(into_received(message).is_none());
    }

    #[test]
    fn a_message_without_a_body_is_kept_with_an_empty_one() {
        let message = Message::builder()
            .message_id("m1")
            .receipt_handle("h1")
            .build();
        assert_eq!(into_received(message).unwrap().body, "");
    }

    #[test]
    fn batch_failures_name_the_message_by_entry_position() {
        let receipts: Vec<Receipt> = ["m1", "m2", "m3"]
            .into_iter()
            .map(|id| Receipt {
                message_id: MessageId::new(id.to_owned()),
                handle: ReceiptHandle::new(format!("h-{id}")),
                receive_count: NonZeroU32::MIN,
            })
            .collect();
        let failed = [
            BatchResultErrorEntry::builder()
                .id("2")
                .code("ReceiptHandleIsInvalid")
                .sender_fault(true)
                .build()
                .unwrap(),
            BatchResultErrorEntry::builder()
                .id("7")
                .code("InternalError")
                .message("try again")
                .sender_fault(false)
                .build()
                .unwrap(),
        ];
        let failures = entry_failures(&failed, receipts.iter());
        assert_eq!(failures.len(), 2);
        assert_eq!(
            failures[0].message_id.as_ref().map(MessageId::as_str),
            Some("m3")
        );
        assert_eq!(failures[0].code, "ReceiptHandleIsInvalid");
        assert!(failures[0].sender_fault);
        assert_eq!(failures[1].message_id, None);
        assert_eq!(failures[1].message.as_deref(), Some("try again"));
    }
}
