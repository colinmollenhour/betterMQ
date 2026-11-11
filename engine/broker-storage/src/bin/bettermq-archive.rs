use broker_storage::{ArchiveSource, ArchiveToolReport};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Verify or restore sealed-segment WAL archives. Active WAL tails need a separate backup.
#[derive(Debug, Parser)]
#[command(name = "bettermq-archive", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Verify checksums, including legacy archives that cannot be restored.
    Scrub { archive_dir: PathBuf },
    /// Restore one source's consistent WAL chain into a new or empty directory.
    Restore {
        archive_dir: PathBuf,
        destination: PathBuf,
        /// Stable archive cluster/node identity, for example production/broker-1.
        #[arg(long, value_parser = parse_source)]
        source: ArchiveSource,
        /// Include only segments archived at or before this Unix millisecond.
        #[arg(long)]
        until_unix_ms: Option<u128>,
    },
    /// Verify S3/MinIO/R2 archives using archive credentials from the environment.
    #[cfg(feature = "s3")]
    ScrubS3 {
        #[arg(default_value = "bettermq-archive")]
        prefix: String,
    },
    /// Restore one S3/MinIO/R2 source into a new or empty directory.
    #[cfg(feature = "s3")]
    RestoreS3 {
        destination: PathBuf,
        #[arg(default_value = "bettermq-archive")]
        prefix: String,
        #[arg(long, value_parser = parse_source)]
        source: ArchiveSource,
        #[arg(long)]
        until_unix_ms: Option<u128>,
    },
}

fn parse_source(value: &str) -> Result<ArchiveSource, String> {
    ArchiveSource::parse(value).map_err(|error| error.to_string())
}

fn print_report(report: ArchiveToolReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize archive report")
    );
    if !report.errors.is_empty() {
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Scrub { archive_dir } => {
            print_report(broker_storage::scrub_local_archive(&archive_dir))
        }
        Command::Restore {
            archive_dir,
            destination,
            source,
            until_unix_ms,
        } => {
            print_report(broker_storage::restore_local_archive_source_until(
                &archive_dir,
                &destination,
                &source,
                until_unix_ms,
            ));
        }
        #[cfg(feature = "s3")]
        Command::ScrubS3 { prefix } => {
            let store = broker_storage::open_archive_object_store_from_env()?;
            let runtime = tokio::runtime::Runtime::new()?;
            print_report(runtime.block_on(broker_storage::scrub_object_archive(store, &prefix)));
        }
        #[cfg(feature = "s3")]
        Command::RestoreS3 {
            destination,
            prefix,
            source,
            until_unix_ms,
        } => {
            let store = broker_storage::open_archive_object_store_from_env()?;
            let runtime = tokio::runtime::Runtime::new()?;
            print_report(
                runtime.block_on(broker_storage::restore_object_archive_source_until(
                    store,
                    &prefix,
                    &destination,
                    &source,
                    until_unix_ms,
                )),
            );
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("bettermq-archive: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_requires_explicit_source_and_valid_timestamp() {
        assert!(Cli::try_parse_from(["bettermq-archive", "restore", "archive", "dest"]).is_err());
        assert!(Cli::try_parse_from([
            "bettermq-archive",
            "restore",
            "archive",
            "dest",
            "--source",
            "cluster/node",
            "--until-unix-ms",
            "bad"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "bettermq-archive",
            "restore",
            "archive",
            "dest",
            "--source",
            "cluster/../node"
        ])
        .is_err());
        let cli = Cli::try_parse_from([
            "bettermq-archive",
            "restore",
            "archive",
            "dest",
            "--source",
            "cluster/node",
            "--until-unix-ms=123",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Restore {
                until_unix_ms: Some(123),
                ..
            }
        ));
    }

    #[test]
    fn help_and_version_are_successful() {
        for option in ["--help", "--version"] {
            let error = Cli::try_parse_from(["bettermq-archive", option]).unwrap_err();
            assert_eq!(error.exit_code(), 0);
        }
    }
}
