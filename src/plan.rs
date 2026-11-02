//! Decides what to do with each message of one received batch. Pure: no I/O and no logging;
//! the executor acts on the plan and logs the reasons it carries.

use std::fmt;

use crate::config::{Config, Project};
use crate::event::{self, EventError, ImageAction};
use crate::message::{Receipt, Received};

const PUSH: &str = "PUSH";
const SUCCESS: &str = "SUCCESS";

/// Every message of the batch is in exactly one of the three lists.
#[derive(Debug)]
pub(crate) struct Plan<'c> {
    /// At most one per project, in config order.
    pub(crate) deploys: Vec<Deploy<'c>>,
    /// Well-formed ECR events that are not ours: delete them.
    pub(crate) ignore: Vec<Ignored>,
    /// Bodies the daemon does not understand: keep them, so they end up in the DLQ.
    pub(crate) retain: Vec<Retained>,
}

/// One deploy of `project`, settling every message that asked for it.
#[derive(Debug)]
pub(crate) struct Deploy<'c> {
    pub(crate) project: &'c Project,
    pub(crate) receipts: Vec<Receipt>,
    /// The distinct pushed digests, in arrival order.
    pub(crate) digests: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct Ignored {
    pub(crate) receipt: Receipt,
    pub(crate) action: ImageAction,
    pub(crate) reason: IgnoreReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IgnoreReason {
    NotPush,
    NotSuccess,
    Untagged,
    NotWatched,
}

impl fmt::Display for IgnoreReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotPush => "action-type is not PUSH",
            Self::NotSuccess => "result is not SUCCESS",
            Self::Untagged => "the image has no tag",
            Self::NotWatched => "no [[project]] watches this repository and tag",
        })
    }
}

#[derive(Debug)]
pub(crate) struct Retained {
    pub(crate) receipt: Receipt,
    pub(crate) error: EventError,
}

struct Match {
    project: usize,
    receipt: Receipt,
    digest: Option<String>,
}

pub(crate) fn plan_batch<'c>(messages: &[Received], config: &'c Config) -> Plan<'c> {
    let projects = config.projects();
    let mut matches = Vec::new();
    let mut ignore = Vec::new();
    let mut retain = Vec::new();
    for message in messages {
        let receipt = message.receipt.clone();
        let action = match event::parse(&message.body) {
            Ok(action) => action,
            Err(error) => {
                retain.push(Retained { receipt, error });
                continue;
            }
        };
        match classify(&action, projects) {
            Ok(project) => matches.push(Match {
                project,
                receipt,
                digest: action.digest,
            }),
            Err(reason) => ignore.push(Ignored {
                receipt,
                action,
                reason,
            }),
        }
    }

    let mut deploys = Vec::new();
    for (index, project) in projects.iter().enumerate() {
        let (mine, rest): (Vec<Match>, Vec<Match>) =
            matches.into_iter().partition(|m| m.project == index);
        matches = rest;
        if mine.is_empty() {
            continue;
        }
        let mut receipts = Vec::with_capacity(mine.len());
        let mut digests = Vec::new();
        for found in mine {
            if let Some(digest) = found.digest
                && !digests.contains(&digest)
            {
                digests.push(digest);
            }
            receipts.push(found.receipt);
        }
        deploys.push(Deploy {
            project,
            receipts,
            digests,
        });
    }

    Plan {
        deploys,
        ignore,
        retain,
    }
}

/// The position of the project this action deploys, or why it deploys none.
fn classify(action: &ImageAction, projects: &[Project]) -> Result<usize, IgnoreReason> {
    if action.action_type != PUSH {
        return Err(IgnoreReason::NotPush);
    }
    if action.result != SUCCESS {
        return Err(IgnoreReason::NotSuccess);
    }
    let Some(tag) = action.tag.as_deref() else {
        return Err(IgnoreReason::Untagged);
    };
    projects
        .iter()
        .position(|project| project.watches(&action.repository, tag))
        .ok_or(IgnoreReason::NotWatched)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        API_UNIT, BLOG_UNIT, PREVIEW_UNIT, config, event_body, message, push_body,
    };

    fn ids(receipts: &[Receipt]) -> Vec<&str> {
        receipts.iter().map(|r| r.message_id.as_str()).collect()
    }

    fn deployed_units<'p>(plan: &'p Plan<'_>) -> Vec<&'p str> {
        plan.deploys
            .iter()
            .map(|d| d.project.unit().as_str())
            .collect()
    }

    fn ignore_reasons(plan: &Plan<'_>) -> Vec<IgnoreReason> {
        plan.ignore.iter().map(|i| i.reason).collect()
    }

    #[test]
    fn an_empty_batch_plans_nothing() {
        let config = config();
        let plan = plan_batch(&[], &config);
        assert!(plan.deploys.is_empty() && plan.ignore.is_empty() && plan.retain.is_empty());
    }

    #[test]
    fn a_tagged_push_of_a_watched_image_deploys_its_project() {
        let config = config();
        let messages = [message(
            "m1",
            push_body("kenespartadev", Some("latest"), "sha256:a"),
            1,
        )];
        let plan = plan_batch(&messages, &config);
        assert_eq!(deployed_units(&plan), [BLOG_UNIT]);
        let [deploy] = plan.deploys.as_slice() else {
            panic!()
        };
        assert_eq!(ids(&deploy.receipts), ["m1"]);
        assert_eq!(deploy.digests, ["sha256:a"]);
        assert!(plan.ignore.is_empty() && plan.retain.is_empty());
    }

    #[test]
    fn several_pushes_of_one_project_make_one_deploy() {
        let config = config();
        let messages = [
            message(
                "m1",
                push_body("kenespartadev", Some("latest"), "sha256:a"),
                1,
            ),
            message(
                "m2",
                push_body("kenespartadev", Some("latest"), "sha256:b"),
                2,
            ),
            message(
                "m3",
                push_body("kenespartadev", Some("latest"), "sha256:a"),
                1,
            ),
        ];
        let plan = plan_batch(&messages, &config);
        let [deploy] = plan.deploys.as_slice() else {
            panic!("expected one deploy, got {:?}", plan.deploys);
        };
        assert_eq!(deploy.project.unit().as_str(), BLOG_UNIT);
        assert_eq!(ids(&deploy.receipts), ["m1", "m2", "m3"]);
        assert_eq!(deploy.digests, ["sha256:a", "sha256:b"]);
    }

    #[test]
    fn a_push_without_a_digest_still_deploys() {
        let config = config();
        let body = push_body("kenespartadev", Some("latest"), "sha256:a")
            .replace(r#""image-digest":"sha256:a","#, "");
        let plan = plan_batch(&[message("m1", body, 1)], &config);
        let [deploy] = plan.deploys.as_slice() else {
            panic!()
        };
        assert!(deploy.digests.is_empty());
    }

    #[test]
    fn deploys_follow_config_order_not_arrival_order() {
        let config = config();
        let messages = [
            message("m1", push_body("budget-api", Some("latest"), "sha256:c"), 1),
            message(
                "m2",
                push_body("kenespartadev", Some("preview"), "sha256:b"),
                1,
            ),
            message(
                "m3",
                push_body("kenespartadev", Some("latest"), "sha256:a"),
                1,
            ),
        ];
        let plan = plan_batch(&messages, &config);
        assert_eq!(deployed_units(&plan), [BLOG_UNIT, API_UNIT, PREVIEW_UNIT]);
    }

    #[test]
    fn one_repository_under_two_tags_maps_to_two_projects() {
        let config = config();
        let messages = [
            message(
                "m1",
                push_body("kenespartadev", Some("preview"), "sha256:b"),
                1,
            ),
            message(
                "m2",
                push_body("kenespartadev", Some("latest"), "sha256:a"),
                1,
            ),
        ];
        let plan = plan_batch(&messages, &config);
        let units: Vec<(&str, Vec<&str>)> = plan
            .deploys
            .iter()
            .map(|d| (d.project.unit().as_str(), ids(&d.receipts)))
            .collect();
        assert_eq!(units, [(BLOG_UNIT, vec!["m2"]), (PREVIEW_UNIT, vec!["m1"])]);
    }

    #[test]
    fn an_untagged_push_is_ignored() {
        let config = config();
        let plan = plan_batch(
            &[message(
                "m1",
                push_body("kenespartadev", None, "sha256:a"),
                1,
            )],
            &config,
        );
        assert!(plan.deploys.is_empty());
        assert_eq!(ignore_reasons(&plan), [IgnoreReason::Untagged]);
    }

    #[test]
    fn a_push_of_an_unwatched_tag_is_ignored() {
        let config = config();
        let plan = plan_batch(
            &[message(
                "m1",
                push_body("kenespartadev", Some("v1.2.0"), "sha256:a"),
                1,
            )],
            &config,
        );
        assert!(plan.deploys.is_empty());
        assert_eq!(ignore_reasons(&plan), [IgnoreReason::NotWatched]);
        assert_eq!(plan.ignore[0].action.tag.as_deref(), Some("v1.2.0"));
    }

    #[test]
    fn a_push_of_an_unwatched_repository_is_ignored() {
        let config = config();
        let plan = plan_batch(
            &[message(
                "m1",
                push_body("someone-else", Some("latest"), "sha256:a"),
                1,
            )],
            &config,
        );
        assert_eq!(ignore_reasons(&plan), [IgnoreReason::NotWatched]);
        assert_eq!(plan.ignore[0].action.repository, "someone-else");
    }

    #[test]
    fn tags_match_case_sensitively() {
        let config = config();
        let plan = plan_batch(
            &[message(
                "m1",
                push_body("kenespartadev", Some("Latest"), "sha256:a"),
                1,
            )],
            &config,
        );
        assert_eq!(ignore_reasons(&plan), [IgnoreReason::NotWatched]);
    }

    #[test]
    fn a_delete_of_a_watched_image_is_ignored() {
        let config = config();
        let body = event_body("DELETE", "SUCCESS", "kenespartadev", Some("latest"));
        let plan = plan_batch(&[message("m1", body, 1)], &config);
        assert!(plan.deploys.is_empty());
        assert_eq!(ignore_reasons(&plan), [IgnoreReason::NotPush]);
    }

    #[test]
    fn a_failed_push_of_a_watched_image_is_ignored() {
        let config = config();
        let body = event_body("PUSH", "FAILURE", "kenespartadev", Some("latest"));
        let plan = plan_batch(&[message("m1", body, 1)], &config);
        assert!(plan.deploys.is_empty());
        assert_eq!(ignore_reasons(&plan), [IgnoreReason::NotSuccess]);
    }

    #[test]
    fn action_and_result_match_exactly() {
        let config = config();
        for (action, result) in [("push", "SUCCESS"), ("PUSH", "Success")] {
            let body = event_body(action, result, "kenespartadev", Some("latest"));
            let plan = plan_batch(&[message("m1", body, 1)], &config);
            assert!(plan.deploys.is_empty(), "{action}/{result} must not deploy");
            assert_eq!(plan.ignore.len(), 1);
        }
    }

    #[test]
    fn an_unparseable_body_is_retained() {
        let config = config();
        let plan = plan_batch(&[message("m1", "not json".to_owned(), 3)], &config);
        assert!(plan.deploys.is_empty() && plan.ignore.is_empty());
        let [retained] = plan.retain.as_slice() else {
            panic!()
        };
        assert_eq!(retained.receipt.message_id.as_str(), "m1");
        assert!(
            matches!(retained.error, EventError::Malformed(_)),
            "got {:?}",
            retained.error
        );
    }

    #[test]
    fn an_event_from_another_source_is_retained() {
        let config = config();
        let body = push_body("kenespartadev", Some("latest"), "sha256:a")
            .replace(r#""source":"aws.ecr""#, r#""source":"custom.deployer""#);
        let plan = plan_batch(&[message("m1", body, 1)], &config);
        assert!(plan.deploys.is_empty() && plan.ignore.is_empty());
        let [retained] = plan.retain.as_slice() else {
            panic!()
        };
        assert!(
            matches!(retained.error, EventError::NotEcrImageAction { .. }),
            "got {:?}",
            retained.error
        );
    }

    #[test]
    fn a_mixed_batch_puts_each_message_in_its_bucket() {
        let config = config();
        let messages = [
            message(
                "deploy-1",
                push_body("kenespartadev", Some("latest"), "sha256:a"),
                1,
            ),
            message("ignore-1", push_body("kenespartadev", None, "sha256:b"), 1),
            message("retain-1", String::new(), 1),
            message(
                "deploy-2",
                push_body("budget-api", Some("latest"), "sha256:c"),
                1,
            ),
            message(
                "ignore-2",
                push_body("budget-api", Some("sha-123"), "sha256:c"),
                1,
            ),
        ];
        let plan = plan_batch(&messages, &config);
        let deployed: Vec<&str> = plan.deploys.iter().flat_map(|d| ids(&d.receipts)).collect();
        let ignored: Vec<&str> = plan
            .ignore
            .iter()
            .map(|i| i.receipt.message_id.as_str())
            .collect();
        let retained: Vec<&str> = plan
            .retain
            .iter()
            .map(|r| r.receipt.message_id.as_str())
            .collect();
        assert_eq!(deployed, ["deploy-1", "deploy-2"]);
        assert_eq!(ignored, ["ignore-1", "ignore-2"]);
        assert_eq!(retained, ["retain-1"]);
    }

    #[test]
    fn the_receive_count_travels_with_the_receipt() {
        let config = config();
        let messages = [
            message(
                "m1",
                push_body("kenespartadev", Some("latest"), "sha256:a"),
                5,
            ),
            message("m2", "{}".to_owned(), 7),
        ];
        let plan = plan_batch(&messages, &config);
        assert_eq!(plan.deploys[0].receipts[0].receive_count.get(), 5);
        assert_eq!(plan.retain[0].receipt.receive_count.get(), 7);
    }

    /// Every truncation of a valid body is planned without panicking, into exactly one bucket.
    #[test]
    fn any_prefix_of_a_body_lands_in_exactly_one_bucket() {
        let config = config();
        let body = push_body("kenespartadev", Some("latest"), "sha256:ä");
        let prefixes = body
            .char_indices()
            .map(|(at, _)| &body[..at])
            .chain([body.as_str()]);
        for prefix in prefixes {
            let plan = plan_batch(&[message("m1", prefix.to_owned(), 1)], &config);
            let placed = plan.deploys.iter().map(|d| d.receipts.len()).sum::<usize>()
                + plan.ignore.len()
                + plan.retain.len();
            assert_eq!(placed, 1, "prefix {prefix:?}");
        }
    }
}
