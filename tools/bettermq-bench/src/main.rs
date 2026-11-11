//! `bettermq-bench`: repeatable load, scale, and resource benchmarks.

mod analysis;
mod api;
mod broker;
mod calibrate;
mod env;
mod loadgen;
mod metrics;
mod model;
mod procfs;
mod recorder;
mod report;
mod rng;
mod runner;
mod scenarios;
mod sink;
mod stats;
mod trial;
mod verify;
mod workload;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use trial::{Profile, RunCtx};

#[derive(Parser)]
#[command(
    name = "bettermq-bench",
    version,
    about = "Repeatable load, scale, and resource benchmarks for BetterMQ"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Cmd {
    /// Run scenarios and write a report.
    Run(RunArgs),
    /// Re-render the report from an existing results.json (or its directory).
    Report {
        results: PathBuf,
        /// Output directory (default: alongside results.json).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Compare two results.json files and flag regressions.
    Compare {
        baseline: PathBuf,
        candidate: PathBuf,
        /// Relative change that counts as a regression/improvement, in percent.
        #[arg(long, default_value_t = 10.0)]
        threshold_pct: f64,
        /// Exit non-zero if any metric regressed past the threshold.
        #[arg(long)]
        fail_on_regression: bool,
        /// Write the comparison as Markdown here instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Write a compact copy of results.json without time series, small enough
    /// to commit as a baseline (`compare` only needs metrics).
    Slim {
        results: PathBuf,
        /// Output file.
        #[arg(long)]
        out: PathBuf,
    },
    /// Recompute ramp headline metrics from stored steps with the current
    /// knee definition and re-render the report.
    Reanalyze {
        results: PathBuf,
        /// Output directory.
        #[arg(long)]
        out: PathBuf,
    },
    /// Pool several results files (e.g. one invocation per scenario and trial,
    /// interleaved with another build) into one multi-trial result.
    Merge {
        inputs: Vec<PathBuf>,
        /// Output directory (results.json plus the full report).
        #[arg(long)]
        out: PathBuf,
    },
    /// List scenarios.
    List,
}

#[derive(clap::Args)]
struct RunArgs {
    /// smoke (~minutes, sanity), standard (~hour, baseline), full (~hours, deep + soak).
    #[arg(long, default_value = "standard")]
    profile: String,
    /// Comma-separated scenario ids, or `all`.
    #[arg(long, default_value = "all")]
    scenarios: String,
    /// Independent trials per scenario (fresh broker each); medians are reported.
    #[arg(long)]
    repeats: Option<u32>,
    /// Workload seed. Same seed + same binary + same machine = same offered load.
    #[arg(long, default_value_t = 20260101)]
    seed: u64,
    /// Output directory for the report and raw data.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Put broker data dirs here (point at the disk you want to test).
    #[arg(long)]
    data_root: Option<PathBuf>,
    /// Broker binary (default: target/release/bettermq).
    #[arg(long)]
    broker_bin: Option<PathBuf>,
    /// WAL fsync mode: group (default), always, os.
    #[arg(long, default_value = "group")]
    fsync: String,
    /// RUST_LOG for the broker. `info` is the shipped default.
    #[arg(long, default_value = "info")]
    rust_log: String,
    /// CPUs for the broker, e.g. `0-7`. `auto` splits the machine in half; `none` disables pinning.
    #[arg(long, default_value = "auto")]
    broker_cpus: String,
    /// CPUs for this harness (load generator + sink). `auto` is the other half.
    #[arg(long, default_value = "auto")]
    bench_cpus: String,
    /// Multiply every offered rate (use <1 on a small machine).
    #[arg(long, default_value_t = 1.0)]
    rate_scale: f64,
    /// Multiply every duration.
    #[arg(long, default_value_t = 1.0)]
    time_scale: f64,
    /// SLO: ACK p99 limit in ms.
    #[arg(long, default_value_t = 50.0)]
    slo_ack_p99_ms: f64,
    /// SLO: end-to-end delivery p99 limit in ms.
    #[arg(long, default_value_t = 1000.0)]
    slo_delivery_p99_ms: f64,
    /// Keep broker data directories after each trial.
    #[arg(long)]
    keep_data: bool,
    /// Name for the broker build under test, shown in comparisons.
    #[arg(long, default_value = "")]
    label: String,
    /// Abort instead of warning when the host is noisy, the CPU governor is not
    /// `performance`, the data dir is on tmpfs, etc. Use for baselines.
    #[arg(long)]
    strict_env: bool,
    /// Skip the load-generator calibration.
    #[arg(long)]
    no_calibrate: bool,
    /// Extra broker environment, KEY=VALUE (repeatable).
    #[arg(long = "broker-env")]
    broker_env: Vec<String>,
}

fn default_broker_bin() -> PathBuf {
    let exe = std::env::current_exe().unwrap_or_default();
    for dir in exe.ancestors() {
        for profile in ["release", "debug"] {
            let cand = dir.join("target").join(profile).join("bettermq");
            if cand.is_file() {
                return cand;
            }
        }
        let sibling = dir.join("bettermq");
        if dir
            .file_name()
            .is_some_and(|n| n == "release" || n == "debug")
            && sibling.is_file()
        {
            return sibling;
        }
    }
    PathBuf::from("target/release/bettermq")
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Cmd::List => {
            println!("{:<16} {:<8} WHAT IT MEASURES", "ID", "MINUTES");
            for s in scenarios::ALL {
                println!(
                    "{:<16} {:>3.0}/{:.0}/{:.0}   {}",
                    s.id, s.minutes.0, s.minutes.1, s.minutes.2, s.title
                );
            }
            println!("\nMINUTES = approximate wall time per trial at smoke/standard/full.");
            Ok(())
        }
        Cmd::Report { results, out } => {
            let path = if results.is_dir() {
                results.join("results.json")
            } else {
                results
            };
            let report = report::load(&path)?;
            let dir = out.unwrap_or_else(|| path.parent().unwrap_or(Path::new(".")).to_path_buf());
            report::write_all(&report, &dir)?;
            eprintln!("report written to {}", dir.display());
            Ok(())
        }
        Cmd::Reanalyze { results, out } => {
            let mut r = report::load(&resolve_results(&results))?;
            runner::reanalyze(&mut r);
            report::write_all(&r, &out)?;
            eprintln!("reanalyzed into {}", out.display());
            Ok(())
        }
        Cmd::Merge { inputs, out } => {
            let reports = inputs
                .iter()
                .map(|p| report::load(&resolve_results(p)))
                .collect::<Result<Vec<_>, _>>()?;
            let merged = runner::merge(reports)?;
            report::write_all(&merged, &out)?;
            eprintln!(
                "merged {} files into {} ({} scenarios)",
                inputs.len(),
                out.display(),
                merged.scenarios.len()
            );
            Ok(())
        }
        Cmd::Slim { results, out } => {
            let mut report = report::load(&resolve_results(&results))?;
            // Committable files must not carry local paths.
            report.meta.data_root = String::new();
            report.meta.broker_binary = String::new();
            report.meta.command_line = String::new();
            for sc in &mut report.scenarios {
                for t in &mut sc.trials {
                    t.series = model::Series::default();
                    t.final_counters.clear();
                }
            }
            std::fs::write(&out, serde_json::to_vec(&report)?)?;
            eprintln!(
                "wrote {} ({} bytes)",
                out.display(),
                std::fs::metadata(&out)?.len()
            );
            Ok(())
        }
        Cmd::Compare {
            baseline,
            candidate,
            threshold_pct,
            fail_on_regression,
            out,
        } => {
            let a = report::load(&resolve_results(&baseline))?;
            let b = report::load(&resolve_results(&candidate))?;
            let cmp = report::compare::compare(&a, &b, threshold_pct / 100.0);
            let md = report::compare::render_markdown(&a, &b, &cmp);
            match out {
                Some(p) => std::fs::write(&p, &md)?,
                None => println!("{md}"),
            }
            if fail_on_regression && (cmp.regressions > 0 || !cmp.hard_failures.is_empty()) {
                bail!(
                    "{} metric(s) regressed past {threshold_pct}%, {} correctness failure(s) in the candidate",
                    cmp.regressions,
                    cmp.hard_failures.len()
                );
            }
            Ok(())
        }
        Cmd::Run(args) => run_cmd(args),
    }
}

fn resolve_results(p: &Path) -> PathBuf {
    if p.is_dir() {
        p.join("results.json")
    } else {
        p.to_path_buf()
    }
}

fn run_cmd(args: RunArgs) -> anyhow::Result<()> {
    let profile = match args.profile.as_str() {
        "smoke" => Profile::Smoke,
        "standard" => Profile::Standard,
        "full" => Profile::Full,
        other => bail!("unknown profile `{other}` (smoke|standard|full)"),
    };
    let broker_bin = args.broker_bin.clone().unwrap_or_else(default_broker_bin);
    if !broker_bin.is_file() {
        bail!(
            "broker binary not found at {}; build it with `cargo build --release -p broker-server` or pass --broker-bin",
            broker_bin.display()
        );
    }
    let broker_bin = broker_bin.canonicalize()?;
    let ids: Vec<String> = if args.scenarios == "all" {
        scenarios::ALL.iter().map(|s| s.id.to_string()).collect()
    } else {
        args.scenarios
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    for id in &ids {
        if scenarios::find(id).is_none() {
            bail!("unknown scenario `{id}`; run `bettermq-bench list`");
        }
    }
    let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
    let out_dir = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("bench-results/{}-{stamp}", profile.name())));
    std::fs::create_dir_all(&out_dir)?;
    let out_dir = out_dir.canonicalize()?;
    let data_root = args
        .data_root
        .clone()
        .unwrap_or_else(|| out_dir.join("trials"));
    std::fs::create_dir_all(&data_root)?;
    let data_root = data_root.canonicalize()?;

    let logical = env::logical_cpus();
    let (auto_broker, auto_bench) = env::default_split(logical).unzip();
    let broker_list = match args.broker_cpus.as_str() {
        "none" => None,
        "auto" => auto_broker.clone(),
        other => Some(other.to_string()),
    };
    let bench_list = match args.bench_cpus.as_str() {
        "none" => None,
        "auto" => auto_bench.clone(),
        other => Some(other.to_string()),
    };
    if let Some(list) = &bench_list {
        env::set_affinity(&env::parse_cpu_list(list)?)
            .context("pin harness CPUs (use --bench-cpus none to disable)")?;
    }
    let broker_cpus = broker_list
        .as_deref()
        .map(env::parse_cpu_list)
        .transpose()?;

    let extra_env = args
        .broker_env
        .iter()
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or_else(|| anyhow::anyhow!("--broker-env expects KEY=VALUE, got `{kv}`"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let slo = model::Slo {
        ack_p99_ms: args.slo_ack_p99_ms,
        delivery_p99_ms: args.slo_delivery_p99_ms,
        ..Default::default()
    };
    let repeats = args.repeats.unwrap_or_else(|| profile.pick(1, 3, 3));
    let ctx = Arc::new(RunCtx {
        broker_bin,
        data_root,
        profile,
        seed: args.seed,
        fsync: args.fsync.clone(),
        rust_log: args.rust_log.clone(),
        broker_cpus,
        slo,
        keep_data: args.keep_data,
        rate_scale: args.rate_scale,
        time_scale: args.time_scale,
        extra_env,
    });
    let plan = runner::RunPlan {
        ctx,
        scenarios: ids,
        repeats,
        calibrate: !args.no_calibrate,
        bench_cpus: bench_list.unwrap_or_else(|| "unpinned".into()),
        command_line: std::env::args().collect::<Vec<_>>().join(" "),
        strict_env: args.strict_env,
        label: args.label.clone(),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let out_for_cb = out_dir.clone();
    let report = runtime.block_on(runner::run(plan, move |partial| {
        // Persist after every scenario so an interrupted run keeps its data.
        if let Err(e) = report::write_json(partial, &out_for_cb) {
            eprintln!("warning: could not save partial results: {e:#}");
        }
    }))?;
    report::write_all(&report, &out_dir)?;
    eprintln!("\nreport: {}", out_dir.join("report.html").display());
    eprintln!("        {}", out_dir.join("report.md").display());
    eprintln!("        {}", out_dir.join("results.json").display());
    Ok(())
}
