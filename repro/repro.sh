#!/bin/sh
# Hermes 2.22.2: a BATCH subscription silently loses messages after a topic update.
# Needs consumer.useTopicMessageSizeEnabled=true (set in ../workload/sut/consumers.yaml).
set -eu
cd "$(dirname "$0")"
M=http://localhost:18090; F=http://localhost:18080; S=http://localhost:19000
OWNER='{"source":"Plaintext","id":"repro"}'
j() { curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' "$@"; }
pub() { for id in "$@"; do code=$(j -X POST "$F/topics/repro.t" -d "{\"id\":$id}"); echo "  publish id=$id -> $code"; done; }
wait_http() { until curl -sf -o /dev/null "$1"; do sleep 2; done; }

docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null
echo "waiting for Hermes..."; wait_http $M/groups; wait_http $F/status/health
j -X POST $M/groups -d '{"groupName":"repro"}' >/dev/null
until [ "$(j -X POST $M/topics -d "{\"name\":\"repro.t\",\"description\":\"d\",\"contentType\":\"JSON\",\"ack\":\"ALL\",\"retentionTime\":{\"duration\":1},\"owner\":$OWNER}")" = 201 ]; do sleep 2; done
j -X POST $M/topics/repro.t/subscriptions -d "{\"topicName\":\"repro.t\",\"name\":\"b\",\"description\":\"d\",\"endpoint\":\"http://subscriber:9000/b\",\"owner\":$OWNER,\"contentType\":\"JSON\",\"deliveryType\":\"BATCH\",\"subscriptionPolicy\":{\"messageTtl\":3600,\"messageBackoff\":200,\"requestTimeout\":2000,\"batchSize\":1,\"batchTime\":100,\"batchVolume\":64000,\"retryClientErrors\":false}}" >/dev/null

echo "1) baseline: publish ids 1-3 and wait for delivery"
until curl -s $S | grep -q '"received": \[.*3'; do pub 1 2 3 >/dev/null; sleep 5; done
curl -s -X POST $S/mode/ok >/dev/null
echo "2) PUT /topics/repro.t {maxMessageSize: 20480}  (consumer reinitializes)"
j -X PUT $M/topics/repro.t -d '{"maxMessageSize":20480}'; echo
until docker compose logs consumers1 2>&1 | grep -q "Reinitializing message receiver"; do sleep 1; done
echo "   consumer logged: Reinitializing message receiver, contentType or messageSize changed."; sleep 3
echo "3) subscriber returns 503; publish ids 101-103"
curl -s -X POST $S/mode/fail >/dev/null; pub 101 102 103; sleep 5
echo "4) subscriber healthy again; publish ids 201-203; wait 60s (TTL is 3600s, retries every 200ms)"
curl -s -X POST $S/mode/ok >/dev/null; pub 201 202 203; sleep 60
echo "5) result:"
curl -s $S | python3 -c '
import json,sys; s=json.load(sys.stdin); got=set(s["received"])
print("   delivery attempts per batch (mode, ids):", [(r["mode"], r["ids"]) for r in s["requests"] if any(i in (101,102,103) for i in r["ids"])])
lost=[i for i in (101,102,103) if i not in got]
print("   delivered:", sorted(i for i in got if i and i>100)); print("   LOST (acked 201, never delivered, never retried):", lost)
sys.exit(1 if lost else 0)' && echo "no loss" || echo "BUG REPRODUCED"
