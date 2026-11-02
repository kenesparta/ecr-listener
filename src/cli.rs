use std::path::PathBuf;

use clap::Parser;

use crate::config::DEFAULT_PATH;

/// Deploys pushed ECR images: long-polls an SQS queue of ECR push events and starts the
/// systemd unit of each project whose image was pushed.
#[derive(Debug, Parser)]
#[command(name = "ecr-listener", version)]
pub struct Cli {
    /// The TOML configuration file.
    #[arg(long, value_name = "PATH", default_value = DEFAULT_PATH)]
    pub config: PathBuf,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn the_command_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn config_defaults_to_the_etc_path() {
        let cli = Cli::try_parse_from(["ecr-listener"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("/etc/ecr-listener/config.toml"));
    }

    #[test]
    fn config_can_be_overridden() {
        let cli = Cli::try_parse_from(["ecr-listener", "--config", "/tmp/c.toml"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("/tmp/c.toml"));
    }

    #[test]
    fn version_is_a_flag() {
        let result = Cli::try_parse_from(["ecr-listener", "--version"]);
        assert!(
            matches!(&result, Err(e) if e.kind() == clap::error::ErrorKind::DisplayVersion),
            "got {result:?}"
        );
    }

    #[test]
    fn positional_arguments_are_rejected() {
        let result = Cli::try_parse_from(["ecr-listener", "/etc/ecr-listener/config.toml"]);
        assert!(
            matches!(&result, Err(e) if e.kind() == clap::error::ErrorKind::UnknownArgument),
            "got {result:?}"
        );
    }
}
