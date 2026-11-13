# Issue #2 acceptance evidence

Measured source: `c129ba9360e2b0bd09d14fcea02b7e666cd44333`, compared with
`aad91ca79076350c4362a0ce3abeb1a5383d54d4`.

**Correctness and ready-backlog acceptance pass.** All 51 candidate invocations
completed without critical findings, unexpected missing disposition, duplicates,
FIFO violations, malformed deliveries, or excess concurrency. This is not a
claim that every latency SLO passes or every metric improves: the regressions
and limitations below remain part of the result.

## Bundle

| File | Contents |
|---|---|
| [comparison.json](comparison.json) | Both pooled reports, per-repeat scalar metrics, integrity, phase summaries, memory, workload parameters, warnings, all 102 invocation contexts, fingerprints, and invocation validation |
| [targeted.json](targeted.json) | Three fixed-volume overload invocations, followed by FIFO, isolation, and an actual 120k ready-backlog check |
| [rejected.json](rejected.json) | Earlier rejected full comparisons, failed/inconclusive overload attempts, and the separate syscall-profile result, including its duplicate notice |

These are review-evidence schemas, not direct inputs to `bettermq-bench compare`.
They omit raw sample time series, process logs, setup codes, and payloads. Raw
report SHA-256 values identify the preserved original reports. Unsuccessful
invocations remain unsuccessful; no missing numerical trial is fabricated.

## Protocol and fingerprints

- Standard profile, three independent repeats per scenario, all 17 scenarios;
  **102 serialized/interleaved invocations** across two builds.
- One immutable `c129ba9` harness drove both builds. Both broker binaries were
  built with Rust **1.99.1**; compatibility/correctness gates used Rust **1.89**.
- Group fsync, unchanged SLOs, drain deadlines, and integrity checks. No other
  local build or verification job was launched during the comparison.
- Shared 16-logical-CPU development host, `powersave` governor, swap in use, and
  variable CPU/storage contention. Every invocation retains its environment
  warnings. Affinity is not storage isolation; these are not quiet-host results.
- RSS values below are MiB, although the harness labels the unit `MB`.

| Binary | SHA-256 |
|---|---|
| Baseline broker | `4113d1825de24372455fdb32a2b6b8a115860512f696adaaebf9a2e8ea0f61ed` |
| Candidate broker | `868ccfdd6602283f33b7a9541df2faab271f1b2f120e66a9b21452d3902b216b` |
| Harness | `f79a4fc1e1e8ef5d9383e4eeef8b5c25ad5ee7c7542d51a7126435400b2b518c` |

Hashes identify the exact measured artifacts, not a promise of identical hashes
from a different build environment.

## Results with valid counterparts

Medians across three repeats; brackets show minimum–maximum, not confidence
intervals. Every ready-backlog repeat on both builds reached **120,001** and
finished with **zero** undelivered records.

| Metric | Baseline | Candidate | Median change |
|---|---:|---:|---:|
| Ready-backlog peak RSS, MiB | 1,497.5 [1,392.6–1,536.6] | 81.9 [79.4–92.9] | −94.5% |
| Ready-backlog RSS slope, bytes/message | 3,994 [3,967–4,207] | 160 [148–222] | −96.0% |
| CPU holding the deepest ready backlog, cores | 0.378 [0.297–0.405] | 0.0283 [0.0267–0.0300] | −92.5% |
| Ready-backlog drain, messages/s | 150.4 [145.6–166.2] | 527.2 [505.6–532.3] | 3.51× |
| Ready-backlog fill ACK p99, ms | 69.2 [63.1–79.4] | 12.4 [11.7–21.5] | −82.1% |
| Memory-cycles final settled RSS, MiB | 423.5 [375.0–444.3] | 83.4 [73.8–88.9] | −80.3% |
| Post-warmup RSS growth, MiB/cycle | 31.45 [30.68–45.24] | 2.05 [1.18–5.72] | −93.5% |
| Serial hot-key throughput, messages/s | 101.0 [99.8–103.7] | 157.7 [83.6–160.2] | +56.1% |
| Healthy-neighbor storm delivery p99, ms | 31.8 [29.1–32.0] | 32.1 [30.4–134.0] | +0.9% |
| Complete drain after SIGKILL restart, s | 134.3 [109.3–135.1] | 37.6 [37.6–62.5] | −72.0% |

The hot-key improvement is a median, not a win in every repeat. The healthy
neighbor has a higher worst observed tail despite a similar median. Retained-RSS
warnings remain: lower absolute RSS does not mean the allocator returns all
growth to the operating system after load.

## Fixed-volume and disposition checks

All three fixed-step overload invocations acknowledged and delivered
**210,001/210,001**, with zero duplicate, ordering, malformed, or concurrency
violations and zero final completion gaps. They exceeded the original failed
workload's 185,347 acknowledgements and met the unchanged step-drain deadline.
All three 12k/s offered steps still failed the ACK latency SLO; complete drain
must not be described as passing that SLO or establishing a 12k/s ceiling.

The focused FIFO and isolation checks accounted for **9,776/9,776** and
**24,372/24,372**. The extra ready-backlog invocation reached and drained
**120,001**, with 95.8 MiB peak RSS and 521.0 messages/s drain. These candidate-only
diagnostics are separate from the standard A/B metrics above.

Scheduled-backlog messages are deliberately delayed for one hour. Their pending
records use `loss_expected_zero=false`; they are not silently counted as
delivered or lost. Mixed/fanout workloads reconcile each DLQ record against its
source delivery rather than subtracting an aggregate DLQ count.

## Regressions and limits

- **No repeatable ramp-SLO throughput win is established.** Candidate single,
  batch, and end-to-end sustained-rate medians are **zero**: two repeats in each
  failed the first two latency steps. Large-body sustained median decreased
  **60.0 → 40.0 messages/s**. Isolated successful repeats reached 21.6k single,
  68.6k batch, and 4.27k end-to-end, but those are not three-repeat ceilings and
  must not replace the zero medians. Both builds show durability tails of
  hundreds of milliseconds, including low-CPU-pressure phases. WAL durability
  telemetry also includes periodic metadata checkpoints; low CPU does not prove
  quiet storage.
- **Scheduled memory is not solved by bounded ready dispatch.** Delayed-backlog
  settled RSS is approximately **697 MiB** on both builds; peak RSS increased
  **904.5 → 977.0 MiB**. Cron-fleet peak RSS increased **130.3 → 326.2 MiB**.
  At the largest cron step, median observed fires also changed **8,513 → 20,195**,
  so this is not equal completed work. The larger footprint is nevertheless a
  measured limitation, not an omitted trial.
- **Some latency/lifecycle costs rose.** Outage-error ACK p99 increased
  **20.1 → 131.5 ms**, and post-SIGKILL ready time **0.134 → 0.765 s**, despite the
  faster complete drain. Host contention differs across repeats; these results
  do not attribute every regression to noise or prove every supported workload
  faster.
- **Overload rejections remain visible.** Eight 503 responses were logged at the
  highest batch ramp step; its request error rate was **0.082%**, below the
  configured 0.1% SLO. They were not dropped from the report. Every acknowledged
  record was accounted for. The intentionally hung graceful-restart phase also
  retains its drain-deadline warnings.
- **Nine baseline invocations failed.** Mixed-day FIFO violations were
  **23/63/11**; outage-timeout left **10,347/11,128/10,472** records without
  measured disposition; all three fanout invocations failed final DLQ accounting
  and produced no numerical trial. Those scenario counterparts are not valid
  numerical improvement claims. Candidate counterparts passed independently.
  In particular, a capped/failed baseline recovery window is not a measured
  recovery time.
- **Historical failures remain rejected.** The earlier FIFO/overload failures
  and the syscall-profile duplicate notice are in `rejected.json`. Integrity
  precedes graceful shutdown: missing measured disposition blocks acceptance
  but does not by itself establish physical WAL loss. Traced timings are not
  acceptance measurements.

## Correctness and compatibility gates

At the measured source, Rust 1.89 formatting, workspace/all-target Clippy with
`-D warnings`, and all **68 workspace result groups** passed. Main unit counts:
benchmark 69, API 145, dispatch 82, partition 89, Raft 44, replication 29, server
33, storage 133. Both explicit real-process RF=3 local/Shared SIGKILL/minISR gates
and the 10k-message crash-durability gate passed. The measured source also passed
[all eight CI checks](https://github.com/colinmollenhour/betterMQ/actions/runs/43285585712).

The changes retain fsync, quorum visibility, FIFO, stable identities, legacy
durable formats, the system allocator, and the all-configured-replica payload-GC
floor. Native I/O admission remains 32. Contended completion can add a cursor-only
write; Shared identity changes still require coordinated whole-cell upgrade.
Unknown-outcome unacknowledged retries remain at-least-once, not exactly-once.

## Reproduction reference

The repository's [benchmark guide](../../README.md) documents building and running
the harness. The measured local invocation used the frozen binaries above:

```bash
BENCH=bench-results/bin/issue-2-bench-c129ba9 \
  bash tools/bettermq-bench/scripts/ab.sh \
    --a bench-results/bin/issue-2-before-aad91ca \
    --b bench-results/bin/issue-2-final-c129ba9 \
    --label-a before-aad91ca --label-b final-c129ba9 \
    --out bench-results/reproduced-c129ba9 --trials 3 --profile standard
```

Fixed-step diagnostics used `--ramp-steps 1 --rate-scale 48 --repeats 1` with
seeds **20275939, 20260101, 20268020**, in that execution order. Standard A/B
invocations did not use `--ramp-steps`. Rebuilding the two source refs with the
same compiler and native dependency settings is required for a fresh comparison;
the measured binaries themselves are not committed in this bundle.
