# Baselines

Slimmed `results.json` files (no time series) for `bettermq-bench compare`.

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
