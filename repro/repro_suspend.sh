#!/bin/sh
# Hermes 2.22.2: a SERIAL subscription keeps delivering long after it is suspended,
# when every inflight slot is held by a message being retried (serial analogue of #1755).
set -eu
cd "$(dirname "$0")"
M=http://localhost:18090; F=http://localhost:18080; S=http://localhost:19000
OWNER='{"source":"Plaintext","id":"repro"}'
j() { curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' "$@"; }
wait_http() { until curl -sf -o /dev/null "$1"; do sleep 2; done; }
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null 2>&1
echo "waiting for Hermes..."; wait_http $M/groups; wait_http $F/status/health
j -X POST $M/groups -d '{"groupName":"repro"}' >/dev/null
until [ "$(j -X POST $M/topics -d "{\"name\":\"repro.s\",\"description\":\"d\",\"contentType\":\"JSON\",\"ack\":\"ALL\",\"retentionTime\":{\"duration\":1},\"owner\":$OWNER}")" = 201 ]; do sleep 2; done
j -X POST $M/topics/repro.s/subscriptions -d "{\"topicName\":\"repro.s\",\"name\":\"s\",\"description\":\"d\",\"endpoint\":\"http://subscriber:9000/s\",\"owner\":$OWNER,\"contentType\":\"JSON\",\"deliveryType\":\"SERIAL\",\"subscriptionPolicy\":{\"rate\":100,\"messageTtl\":3600,\"messageBackoff\":1000,\"requestTimeout\":1000,\"socketTimeout\":0,\"inflightSize\":1,\"backoffMultiplier\":1.0,\"backoffMaxIntervalInSec\":600,\"retryClientErrors\":false}}" >/dev/null
echo "1) baseline delivery"
until curl -s $S | grep -q '"received": \[1'; do j -X POST $F/topics/repro.s -d '{"id":1}' >/dev/null; sleep 5; done
echo "2) subscriber returns 503; publish id 2 (fills the single inflight slot with a retrying message)"
curl -s -X POST $S/mode/fail >/dev/null; j -X POST $F/topics/repro.s -d '{"id":2}'; echo; sleep 5
echo "3) suspend the subscription"
j -X PUT $M/topics/repro.s/subscriptions/s/state -d '"SUSPENDED"'; echo; T0=$(date +%s)
curl -s $M/topics/repro.s/subscriptions/s | python3 -c 'import json,sys; print("   management reports state:", json.load(sys.stdin)["state"])'
echo "4) wait 360s while suspended"
sleep 360
curl -s $S | python3 -c "
import json,sys; s=json.load(sys.stdin); t0=$T0
late=[r for r in s['requests'] if r['t'] > t0 + 30]
print('   POSTs to the subscriber more than 30s after the confirmed suspend:', len(late))
if late: print('   last one %.0fs after suspend, ids %s' % (late[-1]['t']-t0, late[-1]['ids']))
import collections; print('   POSTs per 30s bucket after suspend:', sorted(collections.Counter(int((r['t']-t0)//30)*30 for r in s['requests'] if r['t']>t0).items()))
sys.exit(0 if late else 1)" && echo "BUG REPRODUCED: Hermes keeps delivering to a SUSPENDED subscription" || echo "not reproduced"
docker compose logs consumers1 2>&1 | grep -E "Stopping main loop|Releasing consumer" | sed 's/^/   /' | cut -c1-160
