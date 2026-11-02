//! Fixtures and hand-written fakes for the planner, executor and poll-loop tests. The fakes
//! record every call into one shared `Journal`, so a test sees the order across both.

use std::collections::{HashMap, VecDeque};
use std::future::pending;
use std::num::NonZeroU32;
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::config::{Config, UnitName};
use crate::message::{MessageId, Receipt, ReceiptHandle, Received};
use crate::queue::{EntryFailure, Queue, QueueError, VisibilityChange};
use crate::systemctl::{StartError, UnitStarter};

pub(crate) const BLOG_UNIT: &str = "personal-infra-deploy@blog.service";
pub(crate) const API_UNIT: &str = "personal-infra-deploy@api.service";
pub(crate) const PREVIEW_UNIT: &str = "personal-infra-deploy@blog-preview.service";

const CONFIG: &str = r#"
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/kenesparta-host-deploy-events"
region = "us-east-1"
deploy_timeout_secs = 300

[[project]]
name = "blog"
repository = "kenespartadev"
tag = "latest"
unit = "personal-infra-deploy@blog.service"

[[project]]
name = "api"
repository = "budget-api"
tag = "latest"
unit = "personal-infra-deploy@api.service"

[[project]]
name = "blog-preview"
repository = "kenespartadev"
tag = "preview"
unit = "personal-infra-deploy@blog-preview.service"
"#;

/// Three projects, two of them on one repository; a 300 s deploy timeout.
pub(crate) fn config() -> Config {
    Config::from_toml(CONFIG).unwrap()
}

/// An EventBridge-delivered ECR image action, shaped like the real ones.
pub(crate) fn event_body(
    action: &str,
    result: &str,
    repository: &str,
    tag: Option<&str>,
) -> String {
    let mut detail = serde_json::json!({
        "result": result,
        "repository-name": repository,
        "image-digest": "sha256:a",
        "action-type": action,
    });
    if let Some(tag) = tag {
        detail["image-tag"] = tag.into();
    }
    serde_json::json!({
        "version": "0",
        "id": "13cde686-328b-6117-af20-0e5566167482",
        "detail-type": "ECR Image Action",
        "source": "aws.ecr",
        "account": "123456789012",
        "time": "2026-11-01T12:00:00Z",
        "region": "us-east-1",
        "resources": [],
        "detail": detail,
    })
    .to_string()
}

/// A successful push of `repository[:tag]` with `digest`.
pub(crate) fn push_body(repository: &str, tag: Option<&str>, digest: &str) -> String {
    event_body("PUSH", "SUCCESS", repository, tag).replace("sha256:a", digest)
}

/// A received message whose receipt handle is `handle-<id>`.
pub(crate) fn message(id: &str, body: String, receive_count: u32) -> Received {
    Received {
        receipt: Receipt {
            message_id: MessageId::new(id.to_owned()),
            handle: ReceiptHandle::new(format!("handle-{id}")),
            receive_count: NonZeroU32::new(receive_count).unwrap(),
        },
        body,
    }
}

pub(crate) fn queue_error() -> QueueError {
    QueueError::Request {
        operation: "Fake",
        source: Box::new(std::io::Error::other("connection reset")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Call {
    /// Receipt handles.
    Delete(Vec<String>),
    /// Receipt handles with their new visibility timeout in seconds.
    ChangeVisibility(Vec<(String, i32)>),
    Start(String),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Journal(Arc<Mutex<Vec<Call>>>);

impl Journal {
    fn record(&self, call: Call) {
        self.0.lock().unwrap().push(call);
    }

    pub(crate) fn calls(&self) -> Vec<Call> {
        self.0.lock().unwrap().clone()
    }
}

/// Answers `receive` from a script; once the script runs out it signals `drained` and then
/// waits forever, like a long poll on an empty queue.
pub(crate) struct FakeQueue {
    journal: Journal,
    inbox: Mutex<VecDeque<Result<Vec<Received>, QueueError>>>,
    pub(crate) drained: Arc<Notify>,
    visibility_fails: bool,
}

impl FakeQueue {
    pub(crate) fn new(journal: &Journal) -> Self {
        Self {
            journal: journal.clone(),
            inbox: Mutex::default(),
            drained: Arc::default(),
            visibility_fails: false,
        }
    }

    pub(crate) fn with_inbox(
        journal: &Journal,
        inbox: impl IntoIterator<Item = Result<Vec<Received>, QueueError>>,
    ) -> Self {
        let queue = Self::new(journal);
        queue.inbox.lock().unwrap().extend(inbox);
        queue
    }

    pub(crate) fn failing_visibility_changes(mut self) -> Self {
        self.visibility_fails = true;
        self
    }
}

impl Queue for FakeQueue {
    async fn receive(&self) -> Result<Vec<Received>, QueueError> {
        let next = self.inbox.lock().unwrap().pop_front();
        match next {
            Some(result) => result,
            None => {
                self.drained.notify_one();
                pending().await
            }
        }
    }

    async fn delete(&self, receipts: &[Receipt]) -> Result<Vec<EntryFailure>, QueueError> {
        let handles = receipts
            .iter()
            .map(|receipt| receipt.handle.as_str().to_owned())
            .collect();
        self.journal.record(Call::Delete(handles));
        Ok(Vec::new())
    }

    async fn change_visibility(
        &self,
        changes: &[VisibilityChange<'_>],
    ) -> Result<Vec<EntryFailure>, QueueError> {
        let changes = changes
            .iter()
            .map(|change| {
                let handle = change.receipt.handle.as_str().to_owned();
                (handle, change.timeout.as_secs())
            })
            .collect();
        self.journal.record(Call::ChangeVisibility(changes));
        if self.visibility_fails {
            Err(queue_error())
        } else {
            Ok(Vec::new())
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Outcome {
    Fail,
    TimeOut,
    /// Never finishes; signals `FakeStarter::hung` first.
    Hang,
}

/// Succeeds for every unit unless told otherwise with `with`.
pub(crate) struct FakeStarter {
    journal: Journal,
    outcomes: HashMap<String, Outcome>,
    pub(crate) hung: Arc<Notify>,
}

impl FakeStarter {
    pub(crate) fn new(journal: &Journal) -> Self {
        Self {
            journal: journal.clone(),
            outcomes: HashMap::new(),
            hung: Arc::default(),
        }
    }

    pub(crate) fn with(mut self, unit: &str, outcome: Outcome) -> Self {
        self.outcomes.insert(unit.to_owned(), outcome);
        self
    }
}

impl UnitStarter for FakeStarter {
    async fn start(&self, unit: &UnitName, timeout: Duration) -> Result<(), StartError> {
        self.journal.record(Call::Start(unit.as_str().to_owned()));
        match self.outcomes.get(unit.as_str()).copied() {
            None => Ok(()),
            Some(Outcome::Fail) => Err(StartError::Failed {
                status: ExitStatus::from_raw(1 << 8),
                stderr: format!(
                    "Job for {unit} failed because the control process exited with error code."
                ),
            }),
            Some(Outcome::TimeOut) => Err(StartError::TimedOut { timeout }),
            Some(Outcome::Hang) => {
                self.hung.notify_one();
                pending().await
            }
        }
    }
}
