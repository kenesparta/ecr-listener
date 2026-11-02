//! `ecr-listener` long-polls an SQS queue that EventBridge fills with ECR push events and, for
//! every pushed image it watches, starts that project's systemd deploy unit. It never talks to
//! Docker or ECR: the unit does the pull. Linux only (systemd, Unix signals).

pub mod cli;
pub mod config;
pub mod daemon;
pub mod telemetry;

mod event;
mod executor;
mod message;
mod plan;
mod queue;
mod retry;
mod systemctl;
#[cfg(test)]
mod testing;
