//! The EventBridge envelope around an ECR "Image Action" event, as the rule delivers it to SQS.
//! Unknown fields are ignored on both levels: EventBridge and ECR add fields over time.

use serde::Deserialize;

const SOURCE: &str = "aws.ecr";
const DETAIL_TYPE: &str = "ECR Image Action";

#[derive(Debug, thiserror::Error)]
pub(crate) enum EventError {
    #[error("the body is not an EventBridge event")]
    Malformed(#[source] serde_json::Error),
    #[error(
        "the event is not an ECR image action (source \"{event_source}\", detail-type \"{detail_type}\")"
    )]
    NotEcrImageAction {
        event_source: String,
        detail_type: String,
    },
    #[error("the ECR image action has a malformed detail")]
    MalformedDetail(#[source] serde_json::Error),
}

#[derive(Debug, Deserialize)]
struct Envelope {
    source: String,
    #[serde(rename = "detail-type")]
    detail_type: String,
    #[serde(default)]
    detail: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct Detail {
    result: String,
    #[serde(rename = "repository-name")]
    repository_name: String,
    #[serde(rename = "action-type")]
    action_type: String,
    #[serde(rename = "image-tag")]
    image_tag: Option<String>,
    #[serde(rename = "image-digest")]
    image_digest: Option<String>,
}

/// The `detail` of an ECR image action, with no judgement yet on whether it matters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImageAction {
    pub(crate) action_type: String,
    pub(crate) result: String,
    pub(crate) repository: String,
    /// Absent for untagged pushes, such as the attestation manifests buildx pushes.
    pub(crate) tag: Option<String>,
    pub(crate) digest: Option<String>,
}

pub(crate) fn parse(body: &str) -> Result<ImageAction, EventError> {
    let envelope: Envelope = serde_json::from_str(body).map_err(EventError::Malformed)?;
    if envelope.source != SOURCE || envelope.detail_type != DETAIL_TYPE {
        return Err(EventError::NotEcrImageAction {
            event_source: envelope.source,
            detail_type: envelope.detail_type,
        });
    }
    let detail: Detail =
        serde_json::from_value(envelope.detail).map_err(EventError::MalformedDetail)?;
    Ok(ImageAction {
        action_type: detail.action_type,
        result: detail.result,
        repository: detail.repository_name,
        tag: detail.image_tag,
        digest: detail.image_digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUSH: &str = r#"{"version":"0","id":"13cde686-328b-6117-af20-0e5566167482","detail-type":"ECR Image Action","source":"aws.ecr","account":"123456789012","time":"2026-11-01T12:00:00Z","region":"us-east-1","resources":[],"detail":{"result":"SUCCESS","repository-name":"kenespartadev","image-digest":"sha256:abc","action-type":"PUSH","image-tag":"latest"}}"#;

    #[test]
    fn a_tagged_push_parses() {
        assert_eq!(
            parse(PUSH).unwrap(),
            ImageAction {
                action_type: "PUSH".to_owned(),
                result: "SUCCESS".to_owned(),
                repository: "kenespartadev".to_owned(),
                tag: Some("latest".to_owned()),
                digest: Some("sha256:abc".to_owned()),
            }
        );
    }

    #[test]
    fn an_untagged_push_has_no_tag() {
        let body = PUSH.replace(r#","image-tag":"latest""#, "");
        assert_eq!(parse(&body).unwrap().tag, None);
    }

    #[test]
    fn unknown_fields_are_tolerated_on_both_levels() {
        let body = PUSH
            .replace(r#""version":"0""#, r#""version":"0","replay-name":"r""#)
            .replace(
                r#""result":"SUCCESS""#,
                r#""result":"SUCCESS","manifest-media-type":"application/vnd.oci.image.index.v1+json","artifact-media-type":"x""#,
            );
        assert_eq!(parse(&body).unwrap().tag.as_deref(), Some("latest"));
    }

    #[test]
    fn a_body_that_is_not_json_is_malformed() {
        let result = parse("deploy blog please");
        assert!(
            matches!(result, Err(EventError::Malformed(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn an_event_without_detail_type_is_malformed() {
        let body = PUSH.replace(r#""detail-type":"ECR Image Action","#, "");
        let result = parse(&body);
        assert!(
            matches!(result, Err(EventError::Malformed(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn another_source_is_not_an_ecr_image_action() {
        let body = PUSH.replace(r#""source":"aws.ecr""#, r#""source":"aws.s3""#);
        let result = parse(&body);
        assert!(
            matches!(&result, Err(EventError::NotEcrImageAction { event_source, .. }) if event_source == "aws.s3"),
            "got {result:?}"
        );
    }

    #[test]
    fn another_ecr_detail_type_is_not_an_image_action() {
        let body = PUSH.replace("ECR Image Action", "ECR Image Scan");
        let result = parse(&body);
        assert!(
            matches!(&result, Err(EventError::NotEcrImageAction { detail_type, .. }) if detail_type == "ECR Image Scan"),
            "got {result:?}"
        );
    }

    #[test]
    fn an_image_action_without_a_repository_has_a_malformed_detail() {
        let body = PUSH.replace(r#""repository-name":"kenespartadev","#, "");
        let result = parse(&body);
        assert!(
            matches!(result, Err(EventError::MalformedDetail(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn an_image_action_without_detail_has_a_malformed_detail() {
        let body = r#"{"source":"aws.ecr","detail-type":"ECR Image Action"}"#;
        let result = parse(body);
        assert!(
            matches!(result, Err(EventError::MalformedDetail(_))),
            "got {result:?}"
        );
    }
}
