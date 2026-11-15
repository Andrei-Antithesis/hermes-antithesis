#!/bin/sh
# Hermes 2.22.2: an idle BATCH subscription receives a POST with an empty body after a topic update.
# Needs consumer.useTopicMessageSizeEnabled=true (set in ../workload/sut/consumers.yaml).
set -eu
cd "$(dirname "$0")"
M=http://localhost:18090; F=http://localhost:18080; S=http://localhost:19000
OWNER='{"source":"Plaintext","id":"repro"}'
j() { curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' "$@"; }
wait_http() { until curl -sf -o /dev/null "$1"; do sleep 2; done; }

docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null
echo "waiting for Hermes..."; wait_http $M/groups; wait_http $F/status/health
j -X POST $M/groups -d '{"groupName":"repro"}' >/dev/null
until [ "$(j -X POST $M/topics -d "{\"name\":\"repro.e\",\"description\":\"d\",\"contentType\":\"JSON\",\"ack\":\"ALL\",\"retentionTime\":{\"duration\":1},\"owner\":$OWNER}")" = 201 ]; do sleep 2; done
j -X POST $M/topics/repro.e/subscriptions -d "{\"topicName\":\"repro.e\",\"name\":\"b\",\"description\":\"d\",\"endpoint\":\"http://subscriber:9000/b\",\"owner\":$OWNER,\"contentType\":\"JSON\",\"deliveryType\":\"BATCH\",\"subscriptionPolicy\":{\"messageTtl\":3600,\"messageBackoff\":200,\"requestTimeout\":2000,\"batchSize\":10,\"batchTime\":500,\"batchVolume\":64000,\"retryClientErrors\":false}}" >/dev/null

echo "1) baseline: publish id 1 and wait for delivery"
until curl -s $S | grep -q '"received": \[1'; do j -X POST $F/topics/repro.e -d '{"id":1}' >/dev/null; sleep 5; done
echo "2) subscription idle for 10s (no traffic)"; sleep 10
N=$(curl -s $S | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["requests"]))')
echo "3) PUT /topics/repro.e {maxMessageSize: 20480}  (consumer reinitializes)"
j -X PUT $M/topics/repro.e -d '{"maxMessageSize":20480}'; echo
until docker compose logs consumers1 2>&1 | grep -q "Reinitializing message receiver"; do sleep 1; done
echo "   consumer logged: Reinitializing message receiver, contentType or messageSize changed."; sleep 10
echo "4) POSTs the subscriber received after the topic update:"
curl -s $S | python3 -c "
import json,sys; s=json.load(sys.stdin); after=s['requests'][$N:]
for r in after: print('   POST body length=%d ids=%s' % (r['len'], r['ids']))
empty=[r for r in after if r['len']==0]
print('   empty-body POSTs:', len(empty))
sys.exit(0 if empty else 1)" && echo "BUG REPRODUCED: empty-body batch POST to an idle subscription" || echo "not reproduced"
