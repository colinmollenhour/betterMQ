# bettermq-bench

A repeatable load, scale, and resource benchmark suite for the BetterMQ broker.
One command starts a fresh broker per trial, drives it with seeded workloads,
checks correctness while it runs, and writes a report that answers:

- **How much can it take?** Sustained ingest and delivery at an explicit SLO.
- **Where does it break down?** Step-load ramps show which limit is hit first
  and what the failure looks like (latency, errors, backlog, generator).
- **What does it cost?** Broker CPU per million messages, RSS, threads, file
  descriptors, disk written and retained, log volume, CPU by thread group.
- **Does it give resources back?** Memory and disk before load, at peak, right
  after draining, and after an idle settle; growth across repeated cycles.
- **Is it still correct under load?** No loss, FIFO order held, parallelism
  respected, duplicates counted, broker log clean.

Output is an HTML report (self-contained, light/dark), a Markdown report for
PRs and CI logs, `results.json` for tooling, and per-second CSV time series.

## Quick start

```bash
cargo build --release -p broker-server -p bettermq-bench

# 3-4 minute sanity pass (does the harness work on this machine?)
target/release/bettermq-bench run --profile smoke --scenarios idle,ingest-single,mixed-day

# The baseline: every scenario, 3 trials each (about 2 hours)
target/release/bettermq-bench run --profile standard --out bench-results/baseline

# Longer soak and deeper backlogs (several hours)
target/release/bettermq-bench run --profile full

target/release/bettermq-bench list                      # scenarios + durations
target/release/bettermq-bench report bench-results/baseline   # re-render
```

Then, after changing the broker:

```bash
target/release/bettermq-bench run --profile standard --out bench-results/candidate
target/release/bettermq-bench compare bench-results/baseline bench-results/candidate \
    --threshold-pct 10 --fail-on-regression
```

Commit a baseline without the bulky time series using
`bettermq-bench slim bench-results/baseline --out tools/bettermq-bench/baselines/<machine>.json`
(`compare` accepts it in place of a results directory).

`compare` is direction-aware (higher throughput is good, higher latency is bad),
ignores changes inside the trial-to-trial noise, and warns when the two runs
used different hardware, seeds, profiles, rate/time scales, SLOs, CPU pinning,
broker settings, or scenario parameters. `--fail-on-regression` also fails on
correctness: a candidate scenario that errors or fails, a new critical finding
(lost messages, FIFO violations), or a metric that vanished or became
non-finite, however fast it was.

Interrupted runs keep partial results but exit unsuccessfully; `merge` rejects
unfinished inputs and comparison gates reject unfinished candidates or missing
scenarios. The interleaved `scripts/ab.sh` helper resumes only invocations with
both results and a successful completion marker, never a partial results file.

## Profiles

| Profile | Purpose | Trials | Approx. time (all scenarios) |
|---|---|---|---|
| `smoke` | Does it run? Shapes only, numbers are rough. | 1 | 25 min |
| `standard` | The baseline to commit and compare against. | 3 | 2 h |
| `full` | Longer days, deeper backlogs (up to 1M), more cycles. | 3 | 6 h+ |

`--strict-env` aborts instead of warning when the host is busy, the governor is
not `performance`, or the data directory is on tmpfs. Use it for baselines.
`--scenarios a,b,c` picks a subset, `--repeats N` overrides trials,
`--rate-scale` and `--time-scale` stretch every rate or duration (use
`--rate-scale 0.5` on a small machine).

## Getting trustworthy numbers

The harness prints and records a warning for each of these. Fix them for a
baseline:

1. **Quiet machine.** The pre-run probe measures how busy the broker's CPUs
   already are and the kernel's CPU pressure (PSI). Other workloads on the host
   inflate CPU time and latency and make runs unrepeatable.
2. **CPU governor** `performance` (`cpupower frequency-set -g performance`).
3. **Real disk for the data directory** (`--data-root /mnt/nvme/bench`). On
   tmpfs fsync is free and durability cost disappears.
4. **CPU split.** With 8 or more logical CPUs the broker gets the first half and
   the load generator + destination sink the second (`--broker-cpus`,
   `--bench-cpus`, `none` to disable). The report shows the harness's own CPU so
   you can see it was not the bottleneck; the calibration step measures the
   generator's ceiling against a null server and the ramps stop (and say so) if
   the generator, not the broker, saturates.
5. **Trials.** Three trials give a spread; the report shows it, and `compare`
   uses it to decide what counts as a real change.

Record `--fsync`, `--rust-log` and payload sizes with every result (the report
does). The shipped defaults are `BETTERMQ_FSYNC=group` and `RUST_LOG=info`;
per-delivery INFO logs are part of the cost of the default configuration and are
reported as log bytes per message. Re-run with `--rust-log warn` to isolate the
engine.

## How measurements are made

- **Open loop, no coordinated omission.** Every message has an intended send
  time from a seeded plan. Latency is measured from that time, so a slow broker
  shows its queueing delay instead of silently slowing the test. Generator lag
  (actual minus intended send time) is recorded separately.
- **Keyed (FIFO) messages** are sent the way a real producer does it: the next
  message for an entity goes out after the previous one was answered.
- **Seeded and deterministic.** A fixed xoshiro256++ stream, stateless per-message
  hashing, and a pad pool give byte-identical offered load for the same seed.
  `--seed` changes the workload.
- **Phases.** Each trial records named phases (baseline idle, warm-up, measure,
  fault, drain, settle) that appear as shaded bands on every chart.
- **Per-second sampling** joins client counters, sink counters, `/proc` data for
  the broker (CPU user/sys, RSS split into anonymous and file-backed, threads,
  fds, context switches, disk read/write bytes, per-thread-group CPU), the data
  directory size, and the broker's `/metrics/prometheus` (fsync p99 and batching,
  in-flight, retries, server-side ACK p99).
- **Verification ledger.** Every payload carries a stream/sequence header. The
  sink keeps a byte per message (about 2 MB per million) to detect loss,
  duplicates, per-key order inversions and over-parallelism with no locks.
- **SLO.** A load step passes when ACK p99, error rate and achieved/offered are
  within limits (`--slo-ack-p99-ms`, `--slo-delivery-p99-ms`). The sustained
  ceiling is the last passing step before the first failure.

## Scenarios

| Id | Question it answers |
|---|---|
| `idle` | What does the broker cost when nothing is happening (40 queues, 60 dormant schedules)? |
| `ingest-single` | Maximum sustained single-request ingest; shape of the breakdown. |
| `ingest-batch` | The same with 100-message batches. |
| `ingest-large` | The same with 64 KB payloads (bandwidth, WAL volume, memory). |
| `e2e-ramp` | When does delivery, not ingest, stop keeping up (20 ms destination)? |
| `mixed-day` | A compressed business day of the realistic workload below: per-family latency, CPU per million messages, memory release. |
| `burst-drain` | 5x burst on nominal load: peak backlog, time to recover. |
| `cron-fleet` | A realistic 50-job schedule, then thousands of aligned and staggered schedules: fire lateness, missed fires. |
| `outage-error` | Destination answers 503 for a while: retry CPU, ingest isolation, catch-up time. |
| `outage-timeout` | Destination hangs: timeouts occupy delivery slots. |
| `noisy-neighbor` | One tenant's destination hangs; does a healthy tenant's latency change? |
| `ordered-lanes` | FIFO order across thousands of lanes; one hot lane's serial throughput. |
| `fanout` | Group fan-out to 7 members, one slow, one flaky, one down mid-run. |
| `backlog-depth` | Memory, disk and ACK latency per queued message as the backlog deepens; drain rate. |
| `backlog-scheduled` | The same for messages scheduled an hour out. |
| `memory-cycles` | Repeated burst/drain/idle cycles: does settled memory, fds, threads or disk grow? |
| `crash-recovery` | SIGKILL and SIGTERM restarts with a backlog: restart time and no acknowledged loss. |

## The realistic workload

`mixed-day` models a multi-tenant order-processing backend. Nothing here is
taken from a real system; the shape is what matters: heavy-tailed, skewed and
bursty.

| Queue | Share | Payload (median) | Handler | Notes |
|---|---|---|---|---|
| `orders.intake` | 18% | 3.2 KB | 40 ms | storefront pushes; 3% idempotent re-sends; hourly import waves; rush/normal/low priorities |
| `orders.lifecycle` | 30% | 650 B | 25 ms | status events, FIFO per order (50k lanes) |
| `inventory.adjust` | 22% | 420 B | 15 ms | FIFO per SKU, Zipf-hot SKUs build deep lanes |
| `changelog.process` | 12% | 220 B | 120 ms | "something changed, recompute"; 35% re-sent with the same idempotency key |
| `carrier.label` | 6% | 2.1 KB | 700 ms | slow external API, 3% transient failures, afternoon cut-off wave |
| `notify.partner` | 6% | 1.1 KB | 250 ms (heavy tail) | 5% transient, 0.5% permanent failures (dead-lettered) |
| `email.send` | 4% | 1.5 KB | 80 ms | low priority, 10% delayed a few seconds |
| `docs.render` | 1.5% | 60 KB (to 900 KB) | 400 ms | large documents |
| `reports.build` | 0.5% | 320 B | 3 s | 60% scheduled for later |

Tenants follow a Zipf(1.05) distribution over 300 merchants. A "day" is
compressed into the run length with a 24-point load curve (quiet night, morning
ramp, midday peak, evening decline). The scheduled-work scenario mirrors a
typical back-office cron mix: many every-minute sweeps, a few every 5-30
minutes, and a long tail of nightly and weekly jobs, with the cluster of jobs
that all fire at the top of the hour.

## Adding a scenario

1. Create `src/scenarios/<name>.rs` with `params(ctx)` and
   `async fn run(ctx, index, seed) -> Result<TrialResult>`.
2. Start a `Trial`, create queues with `trial.queue(..)`, register ledger
   streams with `trial.verifier.register(..)`, drive load with
   `trial.drive(cfg, |t0| plan)`, mark phases with `trial.phase(name, kind)`,
   push `Metric`s (give each a direction so `compare` knows good from bad) and
   `Table`s, and call `trial.finish()`.
3. Add an `Info` entry and the dispatch arms in `src/scenarios/mod.rs`.

Metric ids are the comparison key: renaming one breaks baseline comparisons.

## Known measurement limits

- Cron lateness is the arrival time modulo the job period, so a fire a whole
  period late is caught by the fire-count check (missed/duplicate fires), not by
  the lateness figure.
- Fan-out accounting maps a delivery to its member by destination index, so the
  `fanout` scenario registers its member destinations first.
- A hot FIFO lane's producer waits for each ACK; the per-hop overhead is only
  reported when the lane was actually backlogged.
- Batch ramps fill batches as load rises (the report gives the mean batch size at
  the sustained step); ACK latency includes up to 2 ms of client batching linger
  and the 1 ms timer granularity of the generator.
- Drain waits give up after 60 s without any delivery progress (a wedged or
  circuit-broken destination); restart, outage, noisy-neighbour and backlog
  scenarios wait the full timeout because the wait is the measurement.
- RSS "before/after" is not reported for scenarios that restart the broker.

## Limits

- Linux only for resource sampling (`/proc`); on other systems throughput and
  latency still work.
- Single node. The harness shares the host with the broker (separated by CPU
  pinning); loopback networking has no latency or loss.
- No TLS and one local API token; the cost of TLS termination is not measured.
- Profiling (`perf`, `strace`) is not run by the harness. CPU-by-thread-group
  tells you which pool burns CPU; use a profiler for why.
- Power-loss durability is not tested, only process crash (SIGKILL) and
  graceful shutdown.
