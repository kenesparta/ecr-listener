//! A message as received from the queue, reduced to what the daemon acts on.

use std::fmt;
use std::num::NonZeroU32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageId(String);

impl MessageId {
    pub(crate) fn new(raw: String) -> Self {
        Self(raw)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The handle of one delivery: SQS issues a new one on every receive, and only the latest
/// deletes or changes the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceiptHandle(String);

impl ReceiptHandle {
    pub(crate) fn new(raw: String) -> Self {
        Self(raw)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// What it takes to delete a message or change its visibility, plus its delivery count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Receipt {
    pub(crate) message_id: MessageId,
    pub(crate) handle: ReceiptHandle,
    /// `ApproximateReceiveCount`: 1 on the first delivery.
    pub(crate) receive_count: NonZeroU32,
}

#[derive(Debug, Clone)]
pub(crate) struct Received {
    pub(crate) receipt: Receipt,
    pub(crate) body: String,
}
