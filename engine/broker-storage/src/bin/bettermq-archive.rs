use broker_storage::{restore_local_archive_until, scrub_local_archive, ArchiveToolReport};
use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage:\n  bettermq-archive scrub <archive-dir>\n  bettermq-archive restore <archive-dir> <destination> [--until-unix-ms N]\n  bettermq-archive scrub-s3 [prefix]\n  bettermq-archive restore-s3 <destination> [prefix] [--until-unix-ms N]\n\n--until-unix-ms restores only segments archived at or before that timestamp (PITR). S3 stays off the ACK path."
    );
    std::process::exit(2);
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

fn take_until(raw: Vec<String>) -> Result<(Vec<String>, Option<u128>), String> {
    let mut until = None;
    let mut positional = Vec::new();
    let mut iter = raw.into_iter();
    while let Some(arg) = iter.next() {
        let value = if arg == "--until-unix-ms" {
            Some(iter.next().ok_or("--until-unix-ms requires a value")?)
        } else if let Some(value) = arg.strip_prefix("--until-unix-ms=") {
            Some(value.to_owned())
        } else if arg.starts_with('-') {
            return Err(format!("unknown option: {arg}"));
        } else {
            positional.push(arg);
            None
        };
        if let Some(value) = value {
            if until.is_some() {
                return Err("--until-unix-ms may only be specified once".into());
            }
            until = Some(value.parse().map_err(|_| {
                format!("invalid --until-unix-ms: {value:?}; expected Unix milliseconds")
            })?);
        }
    }
    Ok((positional, until))
}

fn restore_args(raw: Vec<String>, min: usize) -> (Vec<String>, Option<u128>) {
    let parsed = take_until(raw).unwrap_or_else(|error| {
        eprintln!("error: {error}");
        std::process::exit(2);
    });
    if !(min..=2).contains(&parsed.0.len()) {
        usage();
    }
    parsed
}

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("scrub") => {
            let root = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            print_report(scrub_local_archive(&root));
        }
        Some("restore") => {
            let raw: Vec<String> = args.collect();
            let (positional, until) = restore_args(raw, 2);
            let root = PathBuf::from(positional.first().cloned().unwrap_or_else(|| usage()));
            let destination = PathBuf::from(positional.get(1).cloned().unwrap_or_else(|| usage()));
            print_report(restore_local_archive_until(&root, &destination, until));
        }
        #[cfg(feature = "s3")]
        Some("scrub-s3") => {
            let prefix = args.next().unwrap_or_else(|| "bettermq-archive".into());
            let store = broker_storage::open_archive_object_store_from_env()
                .unwrap_or_else(|error| panic!("open archive object store: {error}"));
            let runtime = tokio::runtime::Runtime::new().expect("archive tooling runtime");
            print_report(runtime.block_on(broker_storage::scrub_object_archive(store, &prefix)));
        }
        #[cfg(feature = "s3")]
        Some("restore-s3") => {
            let raw: Vec<String> = args.collect();
            let (positional, until) = restore_args(raw, 1);
            let destination = PathBuf::from(positional.first().cloned().unwrap_or_else(|| usage()));
            let prefix = positional
                .get(1)
                .cloned()
                .unwrap_or_else(|| "bettermq-archive".into());
            let store = broker_storage::open_archive_object_store_from_env()
                .unwrap_or_else(|error| panic!("open archive object store: {error}"));
            let runtime = tokio::runtime::Runtime::new().expect("archive tooling runtime");
            print_report(
                runtime.block_on(broker_storage::restore_object_archive_until(
                    store,
                    &prefix,
                    &destination,
                    until,
                )),
            );
        }
        _ => usage(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_pitr_never_becomes_unrestricted_restore() {
        for args in [
            vec!["--until-unix-ms"],
            vec!["--until-unix-ms", "typo"],
            vec!["--until-unix-ms="],
            vec!["--until-unix-ms=-1"],
            vec!["--until-unix-ms=340282366920938463463374607431768211456"],
            vec!["--until-unix-ms=1", "--until-unix-ms=2"],
            vec!["--unknown"],
        ] {
            assert!(take_until(args.into_iter().map(String::from).collect()).is_err());
        }
        let (paths, until) =
            take_until(vec!["archive".into(), "--until-unix-ms=123".into()]).unwrap();
        assert_eq!(paths, vec!["archive"]);
        assert_eq!(until, Some(123));
        assert_eq!(take_until(vec![]).unwrap().1, None);
    }
}
