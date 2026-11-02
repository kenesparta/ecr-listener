//! The configuration file. It is read once at startup, deserialized into raw serde shapes and
//! validated into the types the daemon runs on, all before anything touches AWS.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// Where the configuration is read from when `--config` is not given.
pub const DEFAULT_PATH: &str = "/etc/ecr-listener/config.toml";

const UNIT_PATTERN: &str = r"^[A-Za-z0-9][A-Za-z0-9@._:-]*\.service$";

const MIN_DEPLOY_TIMEOUT_SECS: u16 = 30;
const MAX_DEPLOY_TIMEOUT_SECS: u16 = 3600;
const DEFAULT_DEPLOY_TIMEOUT_SECS: u16 = 360;
const _: () = assert!(
    MIN_DEPLOY_TIMEOUT_SECS <= DEFAULT_DEPLOY_TIMEOUT_SECS
        && DEFAULT_DEPLOY_TIMEOUT_SECS <= MAX_DEPLOY_TIMEOUT_SECS
);

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid configuration in {}", path.display())]
    Invalid {
        path: PathBuf,
        #[source]
        source: InvalidConfig,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidConfig {
    #[error("malformed TOML")]
    Toml(#[source] toml::de::Error),
    #[error("queue_url must start with \"https://\"")]
    QueueUrlScheme,
    #[error("region must not be empty")]
    EmptyRegion,
    #[error(
        "deploy_timeout_secs must be between {MIN_DEPLOY_TIMEOUT_SECS} and {MAX_DEPLOY_TIMEOUT_SECS}, got {0}"
    )]
    DeployTimeoutOutOfRange(u64),
    #[error("at least one [[project]] is required")]
    NoProjects,
    /// `number` counts the `[[project]]` tables from 1, in file order.
    #[error("[[project]] #{number} is invalid")]
    Project {
        number: usize,
        #[source]
        source: FieldError,
    },
    #[error("project name \"{0}\" is used by more than one [[project]]")]
    DuplicateName(String),
    #[error("projects \"{first}\" and \"{second}\" both watch {repository}:{tag}")]
    DuplicateImage {
        first: String,
        second: String,
        repository: String,
        tag: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FieldError {
    #[error("{0} must not be empty")]
    Empty(&'static str),
    #[error("unit \"{0}\" must match {UNIT_PATTERN}")]
    Unit(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    queue_url: String,
    region: String,
    deploy_timeout_secs: Option<u64>,
    // Absent and empty are the same mistake; validation reports both as `NoProjects`.
    #[serde(default, rename = "project")]
    projects: Vec<RawProject>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProject {
    name: String,
    repository: String,
    tag: String,
    unit: String,
}

#[derive(Debug)]
pub struct Config {
    queue_url: QueueUrl,
    region: Region,
    deploy_timeout: DeployTimeout,
    /// Never empty, in file order: deploys run in this order.
    projects: Vec<Project>,
}

impl Config {
    /// Reads and validates the file at `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&text).map_err(|source| ConfigError::Invalid {
            path: path.to_owned(),
            source,
        })
    }

    pub fn from_toml(text: &str) -> Result<Self, InvalidConfig> {
        let raw: RawConfig = toml::from_str(text).map_err(InvalidConfig::Toml)?;
        Self::try_from(raw)
    }

    pub(crate) fn queue_url(&self) -> &QueueUrl {
        &self.queue_url
    }

    pub(crate) fn region(&self) -> &Region {
        &self.region
    }

    pub(crate) fn deploy_timeout(&self) -> DeployTimeout {
        self.deploy_timeout
    }

    pub(crate) fn projects(&self) -> &[Project] {
        &self.projects
    }
}

impl TryFrom<RawConfig> for Config {
    type Error = InvalidConfig;

    fn try_from(raw: RawConfig) -> Result<Self, Self::Error> {
        let queue_url = QueueUrl::try_from(raw.queue_url)?;
        let region = Region::try_from(raw.region)?;
        let deploy_timeout = raw
            .deploy_timeout_secs
            .map_or(Ok(DeployTimeout::DEFAULT), DeployTimeout::try_from)?;
        if raw.projects.is_empty() {
            return Err(InvalidConfig::NoProjects);
        }
        let projects = raw
            .projects
            .into_iter()
            .enumerate()
            .map(|(index, project)| {
                Project::try_from(project).map_err(|source| InvalidConfig::Project {
                    number: index + 1,
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        check_unique(&projects)?;
        Ok(Self {
            queue_url,
            region,
            deploy_timeout,
            projects,
        })
    }
}

fn check_unique(projects: &[Project]) -> Result<(), InvalidConfig> {
    let mut names = HashSet::new();
    let mut images: HashMap<(&str, &str), &str> = HashMap::new();
    for project in projects {
        let name = project.name.as_str();
        if !names.insert(name) {
            return Err(InvalidConfig::DuplicateName(name.to_owned()));
        }
        let image = (project.repository.as_str(), project.tag.as_str());
        if let Some(first) = images.insert(image, name) {
            return Err(InvalidConfig::DuplicateImage {
                first: first.to_owned(),
                second: name.to_owned(),
                repository: image.0.to_owned(),
                tag: image.1.to_owned(),
            });
        }
    }
    Ok(())
}

/// One `[[project]]`: the image it watches and the unit that deploys it.
#[derive(Debug)]
pub(crate) struct Project {
    name: ProjectName,
    repository: RepositoryName,
    tag: ImageTag,
    unit: UnitName,
}

impl Project {
    pub(crate) fn name(&self) -> &ProjectName {
        &self.name
    }

    pub(crate) fn unit(&self) -> &UnitName {
        &self.unit
    }

    /// Exact, case-sensitive match on both halves, as ECR reports them.
    pub(crate) fn watches(&self, repository: &str, tag: &str) -> bool {
        self.repository.as_str() == repository && self.tag.as_str() == tag
    }
}

impl TryFrom<RawProject> for Project {
    type Error = FieldError;

    fn try_from(raw: RawProject) -> Result<Self, Self::Error> {
        Ok(Self {
            name: ProjectName::try_from(raw.name)?,
            repository: RepositoryName::try_from(raw.repository)?,
            tag: ImageTag::try_from(raw.tag)?,
            unit: UnitName::try_from(raw.unit)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueUrl(String);

impl QueueUrl {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for QueueUrl {
    type Error = InvalidConfig;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        if raw.starts_with("https://") {
            Ok(Self(raw))
        } else {
            Err(InvalidConfig::QueueUrlScheme)
        }
    }
}

impl fmt::Display for QueueUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The AWS region of the queue. Always taken from the file, never from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Region(String);

impl Region {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Region {
    type Error = InvalidConfig;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        if raw.is_empty() {
            Err(InvalidConfig::EmptyRegion)
        } else {
            Ok(Self(raw))
        }
    }
}

/// The client-side cap on one `systemctl start`, in whole seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeployTimeout(u16);

impl DeployTimeout {
    pub(crate) const DEFAULT: Self = Self(DEFAULT_DEPLOY_TIMEOUT_SECS);

    pub(crate) fn as_secs(self) -> u16 {
        self.0
    }

    pub(crate) fn as_duration(self) -> Duration {
        Duration::from_secs(u64::from(self.0))
    }
}

impl TryFrom<u64> for DeployTimeout {
    type Error = InvalidConfig;

    fn try_from(secs: u64) -> Result<Self, Self::Error> {
        u16::try_from(secs)
            .ok()
            .filter(|secs| (MIN_DEPLOY_TIMEOUT_SECS..=MAX_DEPLOY_TIMEOUT_SECS).contains(secs))
            .map(Self)
            .ok_or(InvalidConfig::DeployTimeoutOutOfRange(secs))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectName(String);

impl ProjectName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProjectName {
    type Error = FieldError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        non_empty(raw, "name").map(Self)
    }
}

impl fmt::Display for ProjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An ECR repository name, compared verbatim with the event's `repository-name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepositoryName(String);

impl RepositoryName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RepositoryName {
    type Error = FieldError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        non_empty(raw, "repository").map(Self)
    }
}

/// An image tag, compared verbatim with the event's `image-tag`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImageTag(String);

impl ImageTag {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ImageTag {
    type Error = FieldError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        non_empty(raw, "tag").map(Self)
    }
}

/// A systemd service unit name. The pattern forbids a leading `-`, so the name can never be
/// read as an option by `systemctl`, even without the `--` the daemon also passes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnitName(String);

impl UnitName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for UnitName {
    type Error = FieldError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        if matches_unit_pattern(&raw) {
            Ok(Self(raw))
        } else {
            Err(FieldError::Unit(raw))
        }
    }
}

impl fmt::Display for UnitName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `UNIT_PATTERN`, by hand: one ASCII alphanumeric, then any of `[A-Za-z0-9@._:-]`, then the
/// `.service` suffix. Unlike many regex engines' `$`, a trailing newline does not match.
fn matches_unit_pattern(unit: &str) -> bool {
    let Some(stem) = unit.strip_suffix(".service") else {
        return false;
    };
    let mut chars = stem.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    first.is_ascii_alphanumeric()
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '@' | '.' | '_' | ':' | '-'))
}

fn non_empty(raw: String, field: &'static str) -> Result<String, FieldError> {
    if raw.is_empty() {
        Err(FieldError::Empty(field))
    } else {
        Ok(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE_URL: &str =
        "https://sqs.us-east-1.amazonaws.com/123456789012/kenesparta-host-deploy-events";

    fn project(name: &str, repository: &str, tag: &str, unit: &str) -> String {
        format!(
            "[[project]]\nname = \"{name}\"\nrepository = \"{repository}\"\ntag = \"{tag}\"\nunit = \"{unit}\"\n"
        )
    }

    fn blog() -> String {
        project(
            "blog",
            "kenespartadev",
            "latest",
            "personal-infra-deploy@blog.service",
        )
    }

    /// The two required top-level keys, then `top`, then `projects`.
    fn file_with(top: &str, projects: &str) -> String {
        format!("queue_url = \"{QUEUE_URL}\"\nregion = \"us-east-1\"\n{top}\n{projects}")
    }

    fn parse(text: &str) -> Result<Config, InvalidConfig> {
        Config::from_toml(text)
    }

    #[test]
    fn the_shipped_example_loads() {
        let config = parse(include_str!("../config.example.toml")).unwrap();
        assert_eq!(config.queue_url().as_str(), QUEUE_URL);
        assert_eq!(config.region().as_str(), "us-east-1");
        assert_eq!(config.deploy_timeout().as_secs(), 360);
        let [project] = config.projects() else {
            panic!("expected one project, got {:?}", config.projects());
        };
        assert_eq!(project.name().as_str(), "blog");
        assert_eq!(
            project.unit().as_str(),
            "personal-infra-deploy@blog.service"
        );
        assert!(project.watches("kenespartadev", "latest"));
    }

    #[test]
    fn deploy_timeout_defaults_to_360_seconds() {
        let config = parse(&file_with("", &blog())).unwrap();
        assert_eq!(config.deploy_timeout(), DeployTimeout::DEFAULT);
        assert_eq!(
            config.deploy_timeout().as_duration(),
            Duration::from_secs(360)
        );
    }

    #[test]
    fn deploy_timeout_accepts_both_limits() {
        for secs in [30, 3600] {
            let config = parse(&file_with(
                &format!("deploy_timeout_secs = {secs}"),
                &blog(),
            ));
            assert_eq!(config.unwrap().deploy_timeout().as_secs(), secs);
        }
    }

    #[test]
    fn deploy_timeout_rejects_one_past_either_limit() {
        for secs in [29, 3601, 0, u64::from(u16::MAX) + 30] {
            let result = parse(&file_with(
                &format!("deploy_timeout_secs = {secs}"),
                &blog(),
            ));
            assert!(
                matches!(result, Err(InvalidConfig::DeployTimeoutOutOfRange(got)) if got == secs),
                "{secs}: got {result:?}"
            );
        }
    }

    #[test]
    fn a_negative_deploy_timeout_is_malformed() {
        let result = parse(&file_with("deploy_timeout_secs = -1", &blog()));
        assert!(
            matches!(result, Err(InvalidConfig::Toml(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn queue_url_must_be_https() {
        for url in [
            "http://sqs.us-east-1.amazonaws.com/1/q",
            "sqs.us-east-1.amazonaws.com/1/q",
            "",
        ] {
            let text = format!("queue_url = \"{url}\"\nregion = \"us-east-1\"\n{}", blog());
            let result = parse(&text);
            assert!(
                matches!(result, Err(InvalidConfig::QueueUrlScheme)),
                "{url:?}: got {result:?}"
            );
        }
    }

    #[test]
    fn region_must_not_be_empty() {
        let text = format!("queue_url = \"{QUEUE_URL}\"\nregion = \"\"\n{}", blog());
        let result = parse(&text);
        assert!(
            matches!(result, Err(InvalidConfig::EmptyRegion)),
            "got {result:?}"
        );
    }

    #[test]
    fn at_least_one_project_is_required() {
        let result = parse(&file_with("", ""));
        assert!(
            matches!(result, Err(InvalidConfig::NoProjects)),
            "got {result:?}"
        );
    }

    #[test]
    fn project_fields_must_not_be_empty() {
        let cases = [
            (project("", "kenespartadev", "latest", "a.service"), "name"),
            (project("blog", "", "latest", "a.service"), "repository"),
            (project("blog", "kenespartadev", "", "a.service"), "tag"),
        ];
        for (text, field) in cases {
            let result = parse(&file_with("", &text));
            assert!(
                matches!(
                    &result,
                    Err(InvalidConfig::Project { number: 1, source: FieldError::Empty(f) }) if *f == field
                ),
                "{field}: got {result:?}"
            );
        }
    }

    #[test]
    fn unit_names_matching_the_pattern_are_accepted() {
        for unit in [
            "personal-infra-deploy@blog.service",
            "a.service",
            "0.service",
            "x:y_z.v-1@i.service",
            "a.service.service",
        ] {
            assert!(
                UnitName::try_from(unit.to_owned()).is_ok(),
                "{unit:?} should be valid"
            );
        }
    }

    #[test]
    fn unit_names_outside_the_pattern_are_rejected() {
        for unit in [
            "-blog.service",
            "--now.service",
            ".service",
            "@blog.service",
            "blog",
            "blog.socket",
            "blog.service\n",
            "blog.service ",
            " blog.service",
            "blog deploy.service",
            "../blog.service",
            "blög.service",
            "",
        ] {
            assert_eq!(
                UnitName::try_from(unit.to_owned()),
                Err(FieldError::Unit(unit.to_owned())),
                "{unit:?} should be invalid"
            );
        }
    }

    #[test]
    fn an_invalid_unit_names_its_project_by_position() {
        let projects = format!(
            "{}{}",
            blog(),
            project("api", "budget-api", "latest", "-x.service")
        );
        let result = parse(&file_with("", &projects));
        assert!(
            matches!(
                &result,
                Err(InvalidConfig::Project { number: 2, source: FieldError::Unit(u) }) if u == "-x.service"
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn project_names_must_be_unique() {
        let projects = format!(
            "{}{}",
            blog(),
            project(
                "blog",
                "budget-api",
                "latest",
                "personal-infra-deploy@api.service"
            )
        );
        let result = parse(&file_with("", &projects));
        assert!(
            matches!(&result, Err(InvalidConfig::DuplicateName(name)) if name == "blog"),
            "got {result:?}"
        );
    }

    #[test]
    fn repository_and_tag_pairs_must_be_unique() {
        let projects = format!(
            "{}{}",
            blog(),
            project(
                "blog2",
                "kenespartadev",
                "latest",
                "personal-infra-deploy@blog2.service"
            )
        );
        let result = parse(&file_with("", &projects));
        assert!(
            matches!(
                &result,
                Err(InvalidConfig::DuplicateImage { first, second, repository, tag })
                    if first == "blog" && second == "blog2" && repository == "kenespartadev" && tag == "latest"
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn a_repository_may_be_watched_under_two_tags() {
        let projects = format!(
            "{}{}",
            blog(),
            project(
                "preview",
                "kenespartadev",
                "preview",
                "personal-infra-deploy@preview.service"
            )
        );
        let config = parse(&file_with("", &projects)).unwrap();
        assert_eq!(config.projects().len(), 2);
    }

    #[test]
    fn a_tag_may_be_watched_in_two_repositories() {
        let projects = format!(
            "{}{}",
            blog(),
            project(
                "api",
                "budget-api",
                "latest",
                "personal-infra-deploy@api.service"
            )
        );
        let config = parse(&file_with("", &projects)).unwrap();
        assert_eq!(config.projects().len(), 2);
    }

    #[test]
    fn unknown_top_level_keys_are_rejected() {
        let result = parse(&file_with("poll_interval = 10", &blog()));
        assert!(
            matches!(result, Err(InvalidConfig::Toml(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn unknown_project_keys_are_rejected() {
        let projects = format!("{}enabled = true\n", blog());
        let result = parse(&file_with("", &projects));
        assert!(
            matches!(result, Err(InvalidConfig::Toml(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn a_missing_project_key_is_malformed() {
        let text =
            "[[project]]\nname = \"blog\"\nrepository = \"kenespartadev\"\ntag = \"latest\"\n";
        let result = parse(&file_with("", text));
        assert!(
            matches!(result, Err(InvalidConfig::Toml(_))),
            "got {result:?}"
        );
    }

    #[test]
    fn project_watches_only_its_exact_repository_and_tag() {
        let config = parse(&file_with("", &blog())).unwrap();
        let [blog] = config.projects() else {
            panic!("expected one project");
        };
        assert!(blog.watches("kenespartadev", "latest"));
        assert!(!blog.watches("kenespartadev", "Latest"));
        assert!(!blog.watches("kenespartadev", "preview"));
        assert!(!blog.watches("kenespartadev-api", "latest"));
    }

    #[test]
    fn a_missing_file_is_a_read_error_naming_the_path() {
        let path = Path::new("/nonexistent/ecr-listener/config.toml");
        let result = Config::load(path);
        assert!(
            matches!(&result, Err(ConfigError::Read { path: p, .. }) if p == path),
            "got {result:?}"
        );
    }
}
