//! Linux `/proc` sampling for the broker process and the host.

use std::path::Path;

#[derive(Clone, Copy, Debug, Default)]
pub struct ProcSample {
    pub cpu_user_s: f64,
    pub cpu_sys_s: f64,
    pub rss_kb: u64,
    pub anon_kb: u64,
    pub file_kb: u64,
    pub swap_kb: u64,
    pub threads: u64,
    pub fds: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub ctx_vol: u64,
    pub ctx_invol: u64,
    pub majflt: u64,
}

pub fn clk_tck() -> f64 {
    // SAFETY: sysconf is a plain libc query with no memory effects.
    let v = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if v > 0 {
        v as f64
    } else {
        100.0
    }
}

fn status_kb(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

pub fn read_proc(pid: i32) -> Option<ProcSample> {
    if pid <= 0 {
        return None;
    }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm may contain spaces/parens; fields resume after the last ')'.
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    // f[0] is field 3 (state); utime is field 14 => f[11].
    let get = |idx: usize| f.get(idx).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    let tck = clk_tck();
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).unwrap_or_default();
    let io_val = |key: &str| {
        io.lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.count() as u64)
        .unwrap_or(0);
    Some(ProcSample {
        cpu_user_s: get(11) as f64 / tck,
        cpu_sys_s: get(12) as f64 / tck,
        majflt: get(9),
        threads: get(17),
        rss_kb: status_kb(&status, "VmRSS:"),
        anon_kb: status_kb(&status, "RssAnon:"),
        file_kb: status_kb(&status, "RssFile:"),
        swap_kb: status_kb(&status, "VmSwap:"),
        fds,
        read_bytes: io_val("read_bytes:"),
        write_bytes: io_val("write_bytes:"),
        ctx_vol: status_kb(&status, "voluntary_ctxt_switches:"),
        ctx_invol: status_kb(&status, "nonvoluntary_ctxt_switches:"),
    })
}

/// Cumulative CPU seconds per thread-name group (trailing digits/dashes stripped,
/// so `tokio-rt-worker-3` and `-7` aggregate).
pub fn read_thread_cpu(pid: i32) -> Vec<(String, f64)> {
    if pid <= 0 {
        return Vec::new();
    }
    let tck = clk_tck();
    let mut by_name: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    let Ok(rd) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return Vec::new();
    };
    for entry in rd.flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let comm = &stat[open + 1..close];
        let f: Vec<&str> = stat[close + 2..].split_whitespace().collect();
        let ticks = f.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
            + f.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        let name = comm.trim_end_matches(|c: char| c.is_ascii_digit() || c == '-' || c == '_');
        let name = if name.is_empty() { comm } else { name };
        *by_name.entry(name.to_string()).or_default() += ticks as f64 / tck;
    }
    by_name.into_iter().collect()
}

/// `some avg10` from the kernel CPU pressure file: percent of the last 10 s
/// during which at least one runnable task was waiting for a CPU.
pub fn read_psi_cpu() -> Option<f64> {
    let text = std::fs::read_to_string("/proc/pressure/cpu").ok()?;
    let line = text.lines().find(|l| l.starts_with("some"))?;
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix("avg10="))
        .and_then(|v| v.parse().ok())
}

/// Busy fraction (0-100) of the given CPUs over `secs`, from /proc/stat.
pub fn measure_cpu_busy(cpus: &[usize], secs: f64) -> Option<f64> {
    fn read(cpus: &[usize]) -> Option<(u64, u64)> {
        let text = std::fs::read_to_string("/proc/stat").ok()?;
        let (mut busy, mut total) = (0u64, 0u64);
        for line in text.lines() {
            let Some(rest) = line.strip_prefix("cpu") else {
                continue;
            };
            let (id, vals) = rest.split_once(' ')?;
            let Ok(id) = id.parse::<usize>() else {
                continue;
            };
            if !cpus.contains(&id) {
                continue;
            }
            let v: Vec<u64> = vals
                .split_whitespace()
                .filter_map(|x| x.parse().ok())
                .collect();
            if v.len() < 5 {
                continue;
            }
            let t: u64 = v.iter().take(8).sum();
            let idle = v[3] + v[4];
            total += t;
            busy += t.saturating_sub(idle);
        }
        (total > 0).then_some((busy, total))
    }
    let a = read(cpus)?;
    std::thread::sleep(std::time::Duration::from_secs_f64(secs));
    let b = read(cpus)?;
    let dt = b.1.saturating_sub(a.1);
    (dt > 0).then(|| 100.0 * b.0.saturating_sub(a.0) as f64 / dt as f64)
}

/// Whole-host busy/total jiffies from the aggregate `cpu` line.
pub fn read_host_cpu() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let line = stat.lines().next()?;
    let v: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|x| x.parse().ok())
        .collect();
    if v.len() < 5 {
        return None;
    }
    let total: u64 = v.iter().take(8).sum();
    let idle = v[3] + v.get(4).copied().unwrap_or(0);
    Some((total.saturating_sub(idle), total))
}

/// Allocated bytes under `dir` (blocks, so sparse/preallocated files count honestly).
pub fn dir_usage_bytes(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in rd.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.blocks() * 512;
            }
        }
    }
    total
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn reads_own_process() {
        let s = read_proc(std::process::id() as i32).expect("self is readable");
        assert!(s.rss_kb > 0);
        assert!(s.threads >= 1);
        assert!(s.fds >= 3);
    }

    #[test]
    fn host_cpu_parses() {
        let (busy, total) = read_host_cpu().unwrap();
        assert!(total >= busy && total > 0);
    }
}
