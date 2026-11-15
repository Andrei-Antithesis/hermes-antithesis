#!/bin/sh
# Local loop: run the workload for $1 seconds (default 300) and collect SDK output into $2.
# Set EVENTUALLY_AFTER_SECS to exercise the eventually instead of disabling it.
set -eu
here="$(cd "$(dirname "$0")/.." && pwd)"
secs="${1:-300}"; out="$(realpath -m "${2:-$here/local/out-$(date +%s)}")"
mkdir -p "$out"
export COMPOSE_FILE="$here/config/docker-compose.yaml:$here/local/docker-compose.local.yaml"
cd "$here/config"
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
timeout 600 sh -c 'until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do sleep 5; done'
echo "setup complete; running ${secs}s"
sleep "$secs"
docker compose cp driver:/tmp/sdk.jsonl "$out/sdk.jsonl" || true
for c in consumers1 consumers2; do docker compose cp $c:/tmp/sdk.jsonl "$out/sdk-$c.jsonl" || true; done
for s in driver consumers1 consumers2 frontend1 management; do docker compose logs --no-color $s > "$out/$s.log" 2>&1; done
docker compose stop >/dev/null
echo "output in $out"
