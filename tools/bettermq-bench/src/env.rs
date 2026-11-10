//! Environment fingerprint and preflight checks.
//!
//! A number is only meaningful next to the machine that produced it, so every
//! report records the hardware, kernel, filesystem, and anything that makes
//! the run noisy (CPU governor, swap, a busy host, a RAM-backed data dir).

use crate::model::EnvInfo;
use std::path::Path;
use std::process::Command;

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn cmd(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

pub fn load_avg_1m() -> f64 {
    read("/proc/loadavg")
        .split_whitespace()
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

/// Total logical CPUs on the machine (not limited by our own affinity mask).
pub fn logical_cpus() -> u32 {
    let n = read("/proc/cpuinfo")
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count() as u32;
    if n > 0 {
        n
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1)
    }
}

fn meminfo_mb(key: &str) -> u64 {
    read("/proc/meminfo")
        .lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

/// `/sys/block/<dev>/queue/rotational` for the device backing `path`, best effort.
fn rotational(path: &Path) -> Option<bool> {
    let source = cmd("df", &["--output=source", &path.to_string_lossy()]);
    let dev = source
        .lines()
        .nth(1)?
        .trim()
        .strip_prefix("/dev/")?
        .to_string();
    let candidates = [
        dev.clone(),
        dev.trim_end_matches(|c: char| c.is_ascii_digit())
            .to_string(),
        dev.trim_end_matches(|c: char| c.is_ascii_digit())
            .trim_end_matches('p')
            .to_string(),
    ];
    for name in candidates {
        let flag = read(&format!("/sys/block/{name}/queue/rotational"));
        match flag.trim() {
            "0" => return Some(false),
            "1" => return Some(true),
            _ => {}
        }
    }
    None
}

pub fn collect(
    data_root: &Path,
    broker_cpus: Option<&[usize]>,
    bench_cpus: Option<&[usize]>,
) -> EnvInfo {
    let cpuinfo = read("/proc/cpuinfo");
    let cpu_model = cpuinfo
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split(':').nth(1))
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    let governor = read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .trim()
        .to_string();
    let fs_type = cmd("stat", &["-f", "-c", "%T", &data_root.to_string_lossy()]);
    let rotational = rotational(data_root);
    let load = load_avg_1m();
    let logical = logical_cpus();
    let mut info = EnvInfo {
        os: std::env::consts::OS.to_string(),
        kernel: read("/proc/sys/kernel/osrelease").trim().to_string(),
        cpu_model,
        logical_cpus: logical,
        mem_total_mb: meminfo_mb("MemTotal:"),
        swap_total_mb: meminfo_mb("SwapTotal:"),
        governor: if governor.is_empty() {
            "n/a".into()
        } else {
            governor
        },
        data_fs_type: if fs_type.is_empty() {
            "unknown".into()
        } else {
            fs_type
        },
        data_device_rotational: rotational,
        load_avg_start: load,
        preflight_busy_broker_cpus_pct: None,
        preflight_busy_bench_cpus_pct: None,
        preflight_cpu_pressure_pct: crate::procfs::read_psi_cpu(),
        rustc: cmd("rustc", &["--version"]),
        warnings: Vec::new(),
    };
    // Probe how busy the cores we are about to use already are.
    let all: Vec<usize> = (0..logical as usize).collect();
    let probe_secs = 1.5;
    let broker_set = broker_cpus.unwrap_or(&all);
    info.preflight_busy_broker_cpus_pct = crate::procfs::measure_cpu_busy(broker_set, probe_secs);
    if let Some(bench) = bench_cpus {
        info.preflight_busy_bench_cpus_pct = crate::procfs::measure_cpu_busy(bench, 0.5);
    }
    let busy_broker = info.preflight_busy_broker_cpus_pct;
    let psi = info.preflight_cpu_pressure_pct;
    let mut warn = |s: String| info.warnings.push(s);
    if let Some(b) = busy_broker {
        if b > 10.0 {
            warn(format!(
                "The CPUs reserved for the broker were already {b:.0}% busy with other work before the run started; results will include contention. Quiesce the host or pick idle cores with --broker-cpus."
            ));
        }
    }
    if let Some(p) = psi {
        if p > 10.0 {
            warn(format!(
                "Kernel CPU pressure (PSI some/avg10) is {p:.0}%: runnable tasks are waiting for CPU on this host."
            ));
        }
    }
    if cfg!(not(target_os = "linux")) {
        warn("Not Linux: process CPU/RSS/IO sampling is unavailable; resource results will be empty.".into());
    }
    let governor = read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor");
    if !governor.trim().is_empty() && governor.trim() != "performance" {
        warn(format!(
            "CPU governor is `{}`, not `performance`; clock speed will vary during the run (set it for tighter repeatability).",
            governor.trim()
        ));
    }
    if load > 0.25 * f64::from(logical) {
        warn(format!(
            "Host load average was {load:.1} on {logical} CPUs at start; other workloads may add noise."
        ));
    }
    if matches!(info.data_fs_type.as_str(), "tmpfs" | "ramfs") {
        warn("Data directory is on a RAM-backed filesystem: fsync is free, so durability cost is hidden. Use a real disk for baselines.".into());
    }
    if info.data_device_rotational == Some(true) {
        warn("Data directory is on a rotational disk; expect fsync-bound results.".into());
    }
    if info.swap_total_mb > 0 {
        let swap_used = meminfo_mb("SwapTotal:").saturating_sub(meminfo_mb("SwapFree:"));
        if swap_used > 256 {
            warn(format!(
                "{swap_used} MB of swap is in use on the host; memory measurements may be distorted."
            ));
        }
    }
    let avail = meminfo_mb("MemAvailable:");
    if avail < 4096 {
        warn(format!(
            "Only {avail} MB of RAM is available; deep-backlog scenarios may be memory-starved."
        ));
    }
    info
}

pub fn git(repo: &Path, args: &[&str]) -> String {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

/// Parse a cpu list such as `0-3,8,10-11`.
pub fn parse_cpu_list(list: &str) -> anyhow::Result<Vec<usize>> {
    let mut out = Vec::new();
    for part in list.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some((a, b)) = part.split_once('-') {
            let (a, b): (usize, usize) = (a.trim().parse()?, b.trim().parse()?);
            anyhow::ensure!(a <= b, "bad cpu range `{part}`");
            out.extend(a..=b);
        } else {
            out.push(part.parse()?);
        }
    }
    anyhow::ensure!(!out.is_empty(), "empty cpu list");
    Ok(out)
}

/// Restrict the calling thread (and threads it spawns later) to `cpus`.
#[cfg(target_os = "linux")]
pub fn set_affinity(cpus: &[usize]) -> anyhow::Result<()> {
    // SAFETY: cpu_set_t is plain data; sched_setaffinity(0, ..) targets the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for cpu in cpus {
            libc::CPU_SET(*cpu, &mut set);
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            anyhow::bail!("sched_setaffinity: {}", std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_affinity(_cpus: &[usize]) -> anyhow::Result<()> {
    anyhow::bail!("CPU pinning is only supported on Linux")
}

/// Split the machine into a broker half and a harness half so the load
/// generator and sink never steal CPU from the system under test.
pub fn default_split(logical: u32) -> Option<(String, String)> {
    if logical < 8 {
        return None;
    }
    let half = logical / 2;
    Some((format!("0-{}", half - 1), format!("{half}-{}", logical - 1)))
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_lists() {
        assert_eq!(parse_cpu_list("0-2,5").unwrap(), vec![0, 1, 2, 5]);
        assert!(parse_cpu_list("3-1").is_err());
        assert!(parse_cpu_list("").is_err());
    }

    #[test]
    fn splits_halves() {
        assert_eq!(default_split(16), Some(("0-7".into(), "8-15".into())));
        assert_eq!(default_split(4), None);
    }
}
