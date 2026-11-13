# Baselines

Slimmed `results.json` files (no time series) for `bettermq-bench compare`.

The [issue #2 acceptance bundle](../evidence/issue-2-c129ba9/README.md) records the
later 17-scenario × 3-repeat comparison, fixed-volume pressure checks, historical
rejections, binary fingerprints, and limitations. Its evidence JSON is not a
direct `compare` input. Ready-backlog RSS and drain improve substantially, but
not every SLO or scheduled-work memory metric improves; the bundle retains those
results rather than presenting a universal throughput win.

| File | Build | Profile | Trials |
|---|---|---|---|
| `barry-main-8435438.json` | `main` @ 8435438 | standard | 3, interleaved with the other build |
| `barry-audit-fixes-9c22605.json` | `audit/fixes` @ 9c22605 | standard | 3 |

```bash
bettermq-bench compare tools/bettermq-bench/baselines/barry-main-8435438.json bench-results/candidate
```

## Read these numbers with care

They were recorded on a shared development machine (16 logical CPUs, CPU governor
`powersave`, load average 6-15, other workloads running, kernel CPU pressure up to
~60% in some trials), not on a quiet benchmark host. Each trial records the host's
CPU busy and pressure (`host.cpu_busy_pct`, `host.cpu_pressure_pct`) and the
report flags contended trials; many trials here are flagged. Expect large
trial-to-trial spread, and treat the trial 2 values (quietest) of most scenarios as
the cleanest. A baseline for gating changes should be re-recorded on a quiet
machine with `--strict-env`:

```bash
tools/bettermq-bench/scripts/ab.sh --a <old broker> --b <new broker> --out bench-results/ab \
    --label-a old --label-b new --trials 3 -- --strict-env
```

The first ramp step often shows a cold-start latency spike on both builds; the
sustained-ceiling metric ignores an isolated failed step (see the README).
Ramp metrics in these files were recomputed with `bettermq-bench reanalyze`, so
write amplification, payload MB/s and log bytes per message are absent for the
ramp scenarios.

## Allocator experiment

Three serialized, interleaved `standard` `memory-cycles` trials compared the
system allocator and mimalloc on identical broker code at `e798277`. Each trial
used five burst/drain/idle cycles. Both builds passed the suite's correctness
checks without critical findings. The slim reports retain per-trial metrics and
environment data, but omit time series:

- [`issue-2-system-e798277.json`](issue-2-system-e798277.json)
- [`issue-2-mimalloc-e798277.json`](issue-2-mimalloc-e798277.json)

| Metric (median across three trials) | System | mimalloc |
|---|---:|---:|
| RSS before load | 35.2 MiB | 68.3 MiB |
| Peak RSS | 125.5 MiB | 217.8 MiB |
| Settled RSS after the final cycle | 125.5 MiB | 202.9 MiB |
| RSS growth per cycle after warmup | 4.39 MiB | 8.46 MiB |

The settled-RSS ranges did not overlap: **109.2–129.7 MiB** for the system
allocator versus **182.0–212.1 MiB** for mimalloc. These were shared-host runs,
and several metrics are marked noisy; they are not a quiet-host throughput
claim. Mimalloc's smaller retained-growth percentage did not offset its larger
absolute footprint. The broker therefore keeps the system allocator, with no
allocator feature or extra native dependency. Bounded queues and blocking-I/O
admission remain necessary regardless of allocator choice.
