#!/usr/bin/env bash
# Isolated fresh volumes; never use this project name for a real deployment.
set -euo pipefail
export BETTERMQ_IMAGE="${1:?image repository required}"
export BETTERMQ_VERSION="${2:?image tag required}"
export BETTERMQ_CLUSTER_SECRET=ci-smoke-secret-not-for-production-0123456789
export BETTERMQ_METRICS_TOKEN=ci-smoke-metrics-not-for-production
DIR="$(cd "$(dirname "$0")/.." && pwd)"
OVERRIDE="$(mktemp)"
PROJECT="bettermq-smoke-$(date +%s)-$$"
cat > "$OVERRIDE" <<'YAML'
services:
  broker1:
    ports: !reset []
  broker2:
    ports: !reset []
  broker3:
    ports: !reset []
YAML
compose() { docker compose -p "$PROJECT" -f "$DIR/docker-compose.ha.yml" -f "$OVERRIDE" "$@"; }
cleanup() {
  result=$?
  if [[ "$result" != 0 ]]; then compose logs --no-color; fi
  compose down --volumes --remove-orphans
  rm -f -- "$OVERRIDE"
  exit "$result"
}
trap cleanup EXIT
compose up -d --no-build --pull never
for node in broker1 broker2 broker3; do
  alive=false
  for _ in $(seq 1 60); do
    if compose exec -T "$node" curl -fsS http://localhost:8080/healthz; then alive=true; break; fi
    sleep 1
  done
  [[ "$alive" == true ]]
  [[ "$(compose exec -T "$node" id -u)" == 1000 ]]
  compose exec -T "$node" sh -ec 'test -w /data; test -w /cluster-shared/meta; touch /cluster-shared/meta/.smoke'
  [[ "$(compose exec -T "$node" curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/internal/v1/cluster/catalog)" == 404 ]]
  [[ "$(compose exec -T "$node" curl -s -o /dev/null -w '%{http_code}' http://localhost:9080/internal/v1/cluster/catalog)" == 401 ]]
done
