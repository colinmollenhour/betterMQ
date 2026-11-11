#!/usr/bin/env bash
# Interleaved A/B benchmark of two broker builds.
#
# Runs every scenario for every trial against build A, then build B, with the
# same seed, alternating which goes first each round. A shared, drifting host
# (other workloads, thermals) then affects both builds alike instead of
# favouring whichever ran during a quiet spell. Resumable: finished
# invocations are skipped.
#
#   ab.sh --a <broker> --b <broker> --out <dir> [--trials 3] [--profile standard]
#         [--scenarios id,id,...] [--seed 20260101] [-- <extra bettermq-bench run args>]
#
# Result: <dir>/a, <dir>/b (pooled multi-trial reports) and <dir>/compare.md.
set -euo pipefail

A=""; B=""; LA="a"; LB="b"; OUT=""; TRIALS=3; PROFILE=standard; SCEN=""; SEED=20260101
while [[ $# -gt 0 ]]; do
  case "$1" in
    --a) A="$2"; shift 2;;
    --b) B="$2"; shift 2;;
    --label-a) LA="$2"; shift 2;;
    --label-b) LB="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --trials) TRIALS="$2"; shift 2;;
    --profile) PROFILE="$2"; shift 2;;
    --scenarios) SCEN="$2"; shift 2;;
    --seed) SEED="$2"; shift 2;;
    --) shift; break;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
[[ -x "$A" && -x "$B" && -n "$OUT" ]] || { echo "need --a, --b (executable brokers) and --out" >&2; exit 2; }
BENCH="${BENCH:-$(dirname "$0")/../../../target/release/bettermq-bench}"
[[ -x "$BENCH" ]] || { echo "bettermq-bench not found at $BENCH (set BENCH=)" >&2; exit 2; }
if [[ -z "$SCEN" ]]; then
  SCEN=$("$BENCH" list | awk 'NR>1 && NF && $1 !~ /^MINUTES/ && $1 != "" {print $1}' | grep -E '^[a-z0-9-]+$' | paste -sd, -)
fi
IFS=',' read -r -a SCENARIOS <<< "$SCEN"
mkdir -p "$OUT"

round=0
for ((t = 0; t < TRIALS; t++)); do
  seed=$((SEED + 7919 * t))
  for sc in "${SCENARIOS[@]}"; do
    order=(a b); if (( (t + round) % 2 == 1 )); then order=(b a); fi
    round=$((round + 1))
    for side in "${order[@]}"; do
      bin="$A"; label="$LA"; [[ "$side" == b ]] && { bin="$B"; label="$LB"; }
      dir="$OUT/runs/$side/t$t-$sc"
      if [[ -f "$dir/results.json" && -f "$dir/.ab-complete" ]]; then continue; fi
      echo "[$(date +%H:%M:%S)] trial $((t + 1))/$TRIALS $sc $side"
      "$BENCH" run --profile "$PROFILE" --repeats 1 --seed "$seed" --scenarios "$sc" \
        --broker-bin "$bin" --label "$label" --out "$dir" --no-calibrate "$@" \
        && touch "$dir/.ab-complete" || echo "  (invocation failed; continuing)"
    done
  done
done

for side in a b; do
  files=("$OUT"/runs/$side/*/results.json)
  "$BENCH" merge "${files[@]}" --out "$OUT/$side"
done
"$BENCH" compare "$OUT/a" "$OUT/b" --out "$OUT/compare.md" || true
echo "done: $OUT/compare.md"
