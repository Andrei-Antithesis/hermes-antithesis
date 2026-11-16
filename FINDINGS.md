# Hermes 2.22.2: findings from Antithesis

Workload in `workload/`. All four findings reproduce on unmodified 2.22.2 in plain
Docker: see `repro/` (includes the verified one-line fix for finding 1).
Earlier run `4902034c…` ended at setup because of a workload classpath problem (fixed).

## 1. Batch subscriptions lose acknowledged messages after a topic update (HIGH)

**Effect:** messages that frontend acknowledged with 201 on an `ack=ALL` topic are never
delivered to a BATCH subscription. They are not logged as discarded either. A SERIAL
subscription on the same topic received every one of them, so the data was in Kafka.

**Mechanism** (`hermes-consumers/.../consumer/BatchConsumer.java`):
- `tearDown()` sets `consuming = false`. `initialize()` never sets it back.
- `updateTopic()` reinitialises with `tearDown(); initialize();` when `contentType` changes, or when
  `maxMessageSize` changes and `consumer.useTopicMessageSizeEnabled=true` (default `false`; enabled in our config).
  Management rejects `contentType` changes except the one-way JSON → Avro migration, so on default config the
  trigger is that migration (same code path; not reproduced separately).
- From then on, the retryer's `retryIfResult(result -> consuming && !result.succeeded() && …)`
  is false. A failed batch (5xx, or 4xx with `retryClientErrors`) returns after one attempt.
  `deliver()` returns normally and `consume()` calls `offerProcessedOffsets(batch)`. The offsets are
  committed on the next commit tick, and the batch's messages are skipped for good.
- The state lasts for the life of that `BatchConsumer` instance, until the subscription is restarted or reassigned.

**Evidence (timeline of hash 9077396278539151813):**
- 105.66: `consumers: batch consumer reinitialized on topic update {"subscription":"at.t0$b0"}` (consumers2)
- 122.99: `batch offsets are marked processed only after delivery…` FAILED:
  `{"consuming":"false","messages":"10","status":"503","subscription":"at.t0$b0"}`
- Eventually (faults stopped, waited 300s): `acked=64 missing=[{"sub":"b0","missing":11,
  "holes_below_max_delivered":11,"max_delivered":358,"first":[314,317,319,323,331,332,338,340,341,343,353]}]`.
  `s0` (SERIAL on the same topic) missed nothing.
- Across the run: 18 hits of the `consuming=false` failure on b0 and b1, 8 of them carrying 1–10 messages.

**Minimal reproduction:**
1. Set `consumer.useTopicMessageSizeEnabled: true` and create a BATCH subscription.
2. `PUT /topics/{t}` with a new `maxMessageSize`. The consumer logs "Reinitializing message receiver".
3. Make the endpoint return 500/503 once, then 200.
4. That batch is never redelivered.

**Fix:** set `consuming = true` in `initialize()`, or reinitialise the receiver without `tearDown()`.
Related history: #1983 (interrupted batch logged as discarded), #1755 → #1974 (still open).

## 2. Serial subscriptions keep delivering long after suspend (MEDIUM, serial counterpart of #1755 → #1974)

The open fix #1974 (copy of #1755, for issue #1645) only changes `BatchConsumer` and `ConsumerProcess`, so it does not
cover this SERIAL path. Plain-Docker repro (`repro/repro_suspend.sh`): 29 POSTs in the first 30s after the confirmed
suspend, then about one a minute until `ConsumerProcessKiller` cancelled the thread 318s after "Stopping main loop".

**Effect:** a SERIAL subscription that was suspended more than 60s earlier still receives a delivery. In
the observed case ZK had shown it SUSPENDED for at least 64s. The only bound is `killAfter` (300s default).

**Mechanism:**
- `ConsumerProcess` STOP only sets `running=false`, which is checked after `consume()` returns.
- `SerialConsumer.consume()` spins in `do { signalsInterrupt.run(); commitIfReady(); } while
  (!pendingOffsets.tryAcquireSlot(...))` while every inflight slot is held by a message being
  retried, so it never returns.
- `ConsumerMessageSender` keeps resending through `retrySingleThreadExecutor`. Only `shutdown()`, from
  `tearDown()`, stops it, and `tearDown()` is never reached until `ConsumerProcessKiller` interrupts
  the thread after `killAfter`.

**Evidence:** hash -259090643421962183, vtime 246.37, `{"sub":"s0","suspended_for_ms":60141}`.
- consumers1 got STOP at 202.85 and logged "Stopping main loop" at 220.35.
- It never logged "Releasing consumer process thread", and its Kafka heartbeat thread kept rejoining.
- Network partitions in the window were all 1.6s or shorter, and the ZK watch was delivered.

**Trigger:** a SERIAL subscription whose endpoint returns 5xx for at least `inflightSize` messages, then a suspend.

## 3. Empty-body batch POST after a topic update (LOW)

`updateTopic()` runs inside `signalsInterrupt` within the *old* `MessageBatchReceiver.next()` loop.
`stop()` sets `receiving=false`, the loop exits, and an empty or partial batch is returned and
delivered. An empty `JsonMessageBatch` has no content, so the subscriber receives a POST with an empty body
(not `[]`). Evidence: hash -1011094325345746026, vtime 61.65, sub b0, body "".

Reproduces deterministically on unmodified 2.22.2 with `repro/repro_empty.sh`: an idle BATCH subscription gets one
empty-body POST per reinitialising topic update. An idle batch normally never leaves the loop (`isExpired()` is false
while empty), so only `stop()` lets an empty one out.
**Fix (verified with the same script):** return early in `BatchConsumer.consume()` when `batch.isEmpty()`
(`repro/fix-batchconsumer-skip-empty-batch.diff`).

## 4. Concurrent startup crash on a fresh ZooKeeper (LOW)

`ZookeeperBasedRepository.ensurePathExists` checks whether the path exists and then creates it. Two nodes starting at once
make one fail with `NodeExistsException` for `/run/hermes/groups`, and that node exits. This was first seen locally;
the workload avoids it by making frontends and consumers wait for management.

`repro/repro_startup.sh` starts management, 2 frontends and 2 consumers at once on a fresh ZooKeeper: 7 of 13 fresh
starts lost a frontend on unmodified 2.22.2. **Fix (verified, 0 of 10 with all nodes healthy):** catch
`KeeperException.NodeExistsException` around the create and treat it as success
(`repro/fix-zookeeper-ensurepathexists-race.diff`).
