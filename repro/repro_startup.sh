#!/bin/sh
# Hermes 2.22.2: nodes started together on a fresh ZooKeeper can crash with NodeExistsException
# (check-then-create race in ZookeeperBasedRepository.ensurePathExists). Runs N fresh starts (default 10).
set -eu
cd "$(dirname "$0")"
N="${1:-10}"; C="docker compose -f docker-compose.yaml -f docker-compose.startup.yaml"
NODES="management frontend1 frontend2 consumers1 consumers2"
crashed=0
for i in $(seq 1 "$N"); do
  $C down -v >/dev/null 2>&1 || true
  $C up -d zk kafka1 kafka2 kafka3 >/dev/null 2>&1; sleep 8        # fresh ZooKeeper and Kafka
  $C up -d $NODES >/dev/null 2>&1; sleep 45                       # all Hermes nodes at once
  bad=""
  for n in $NODES; do
    if $C logs $n 2>&1 | grep -q "NodeExistsException"; then
      state=$($C ps -a --format '{{.State}}' $n 2>/dev/null)
      bad="$bad $n($state)"
      where=$($C logs $n 2>&1 | grep -A40 "NodeExistsException" | grep -o -m1 -E "NodeExists for /[^ ]*|at pl\.allegro[^(]*ensurePathExists" | head -1)
      [ -z "${firstwhere:-}" ] && firstwhere="$where"
    fi
  done
  if [ -n "$bad" ]; then crashed=$((crashed+1)); echo "  start $i: NodeExistsException in$bad"; else echo "  start $i: ok"; fi
done
$C down -v >/dev/null 2>&1 || true
echo "fresh starts with a NodeExistsException: $crashed / $N"
[ -n "${firstwhere:-}" ] && echo "  e.g. $firstwhere"
[ "$crashed" -gt 0 ] && echo "BUG REPRODUCED" || echo "not reproduced"
