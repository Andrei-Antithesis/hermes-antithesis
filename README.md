# Hermes 2.22.2: at-least-once delivery bugs, found with Antithesis

In Hermes 2.22.2, after a topic update that reinitialises the batch receiver, a BATCH subscription can
permanently skip messages that the frontend acknowledged with `201`. The messages are never delivered, never retried and
never logged as discarded, and their offsets are committed. The fix is one line. All four bugs reproduce on the
**unmodified 2.22.2 code** with plain Docker in minutes (configuration used is noted under each bug).

| # | Bug | Impact | Reproduce | Fix |
|---|-----|--------|-----------|-----|
| 1 | After a topic update, a batch consumer never retries a failed batch | **Silent message loss** on BATCH subscriptions | [`repro/repro.sh`](repro/repro.sh) | [1 line](repro/fix-batchconsumer-consuming-flag.diff), verified |
| 2 | A SERIAL subscription keeps delivering for up to `killAfter` (300s) after a suspend | Deliveries to a subscriber that asked to stop | [`repro/repro_suspend.sh`](repro/repro_suspend.sh) | sketched below |
| 3 | Empty-body batch POST after a topic update | Idle subscriber receives a POST with an empty body, not `[]` | [`repro/repro_empty.sh`](repro/repro_empty.sh) | [4 lines](repro/fix-batchconsumer-skip-empty-batch.diff), verified |
| 4 | Concurrent startup on a fresh ZooKeeper: `NodeExistsException` | Node exits at startup (7 of 13 fresh starts) | [`repro/repro_startup.sh`](repro/repro_startup.sh) | [3 lines](repro/fix-zookeeper-ensurepathexists-race.diff), verified |

---

## Bug 1: batch subscriptions lose acknowledged messages

```
$ repro/build-images.sh && repro/repro.sh
2) PUT /topics/repro.t {maxMessageSize: 20480}  (consumer reinitializes)
3) subscriber returns 503; publish ids 101-103
  publish id=101 -> 201
  publish id=102 -> 201
  publish id=103 -> 201
4) subscriber healthy again; publish ids 201-203; wait 60s (TTL is 3600s, retries every 200ms)
5) result:
   delivery attempts per batch (mode, ids): [('fail', [101]), ('fail', [102]), ('fail', [103])]
   delivered: [201, 202, 203]
   LOST (acked 201, never delivered, never retried): [101, 102, 103]
BUG REPRODUCED

$ CONSUMERS_TAG=fixed-2.22.2 repro/repro.sh      # same script, one-line fix applied
   delivery attempts per batch (mode, ids): [('fail', [101]), ('fail', [101]), ... ~20 more failures, one every 200ms ..., ('ok', [101]), ...]
   delivered: [101, 102, 103, 201, 202, 203]
   LOST (acked 201, never delivered, never retried): []
no loss
```

**Root cause** (`hermes-consumers/.../consumer/BatchConsumer.java`):

```java
public void updateTopic(Topic newTopic) {
  ...
  tearDown();      // sets consuming = false
  initialize();    // never sets it back
}

// the retryer only retries while consuming is true:
.retryIfResult(result -> consuming && !result.succeeded() && ...)
```

From then on, the first failed attempt of every batch is treated as the final result. `deliver()` returns normally,
`consume()` calls `offerProcessedOffsets(batch)`, and the offsets are committed on the next tick.
This lasts for the life of that consumer instance, until the subscription is restarted or reassigned.

**Fix:**

```diff
   public void initialize() {
+    consuming = true;
     loadRecorder.initialize();
```

**Trigger:** any topic update that makes `BatchConsumer.updateTopic()` reinitialise the receiver:
- a `maxMessageSize` change while `consumer.useTopicMessageSizeEnabled: true` (off by default). This is the path the
  repro and the Antithesis runs use: both enable the flag in [`consumers.yaml`](workload/sut/consumers.yaml).
- a `contentType` change. Management only allows this as the one-way JSON → Avro migration (`migratedFromJsonType`), so
  migrating a topic that has BATCH subscriptions goes through the same `tearDown(); initialize();` code path. We
  identified this from the code but did not reproduce it separately.

After that, a single failed delivery is enough to lose the batch.

## Bug 2: a SERIAL subscription keeps delivering after suspend

```
$ repro/repro_suspend.sh
3) suspend the subscription
   management reports state: SUSPENDED
4) wait 360s while suspended
   POSTs to the subscriber more than 30s after the confirmed suspend: 5
   last one 330s after suspend, ids [1]
   POSTs per 30s bucket after suspend: [(0, 29), (60, 1), (120, 1), (180, 1), (240, 1), (300, 1)]
BUG REPRODUCED: Hermes keeps delivering to a SUSPENDED subscription
   ... 15:45:35 ConsumerProcess : Stopping main loop for consumer repro.s$s.
   ... 15:50:53 ConsumerProcess : Releasing consumer process thread of subscription repro.s$s   (318s later)
```

After management confirmed `SUSPENDED`, the subscriber got 29 POSTs in the first 30s, then about one a minute, until
`ConsumerProcessKiller` cancelled the thread more than 5 minutes later. The exact counts vary from run to run.

STOP only sets `running=false`, which is checked after `consume()` returns. When every inflight slot is held by a
message being retried, `SerialConsumer.consume()` spins in `tryAcquireSlot` and never returns. The retry executor keeps
resending until `ConsumerProcessKiller` interrupts the thread after `killAfter` (300s by default).

This is the SERIAL counterpart of a problem you already know about. Issue #1645 led to
[#1755](https://github.com/allegro/hermes/pull/1755) (Oct 2023, approved Nov 2023), which was re-opened as
[#1974](https://github.com/allegro/hermes/pull/1974) in Aug 2025 and is still open. That fix only touches
`BatchConsumer` and `ConsumerProcess`, so even once merged it won't stop the SERIAL path shown here.
Possible fix: check `running` inside the `tryAcquireSlot` loop, or shut down the retry executor when STOP is received.

---

## Bug 3: an idle batch subscription gets an empty POST after a topic update

```
$ repro/repro_empty.sh
2) subscription idle for 10s (no traffic)
3) PUT /topics/repro.e {maxMessageSize: 20480}  (consumer reinitializes)
4) POSTs the subscriber received after the topic update:
   POST body length=0 ids=[]
   POST body length=137 ids=[1]
   empty-body POSTs: 1
BUG REPRODUCED: empty-body batch POST to an idle subscription

$ CONSUMERS_TAG=fixed-empty-2.22.2 repro/repro_empty.sh   # 4-line fix applied
   POST body length=137 ids=[1]
   empty-body POSTs: 0
```

`updateTopic()` runs inside the batch-building loop of the old `MessageBatchReceiver` (via `signalsInterrupt`).
Its `tearDown()` calls `receiver.stop()`, the loop exits, and the batch is delivered even if it is empty. Normally an
empty batch never leaves the loop, because `isExpired()` is false while it has no messages. An empty `JsonMessageBatch`
has no content, so the subscriber gets a POST with an empty body. Same trigger conditions as bug 1.

**Fix:** skip empty batches in `BatchConsumer.consume()`; the batch is still released in the `finally` block.

```diff
       inflight.ifPresent(
           batch -> {
+            if (batch.isEmpty()) {
+              // receiver was stopped (e.g. by updateTopic) before any message arrived
+              return;
+            }
```

## Bug 4: nodes started together on a fresh ZooKeeper crash

```
$ repro/repro_startup.sh 10          # management, 2 frontends, 2 consumers started at once, 10 fresh starts
  start 1: NodeExistsException in frontend2(exited)
  ...
fresh starts with a NodeExistsException: 5 / 10
  e.g. NodeExists for /run/hermes/groups
BUG REPRODUCED

$ HERMES_TAG=fixed-zk-2.22.2 repro/repro_startup.sh 10      # 3-line fix applied
fresh starts with a NodeExistsException: 0 / 10
```

Across 13 fresh starts on the release, 7 lost a frontend. With the fix, 10 of 10 started cleanly and every node was
healthy. `ZookeeperBasedRepository.ensurePathExists` checks whether the path exists and then creates it, so two nodes
starting together can both see it missing; the slower one gets `NodeExistsException` and exits.

**Fix:** treat "already exists" as success.

```diff
       try {
         zookeeper.create().creatingParentsIfNeeded().forPath(path);
+      } catch (KeeperException.NodeExistsException e) {
+        // another node created it between our check and create: that's fine
       } catch (Exception e) {
```

---

## How we found them

1. **We aimed where you already invest.** Your history shows a long series of at-least-once and offset-commit fixes:
   #1040, #1053, #1862 → #1891 → #1908, #1983, and the still-open #1755 → #1974. We targeted that state space instead of
   fuzzing at random.
2. **We ran the real cluster under Antithesis.** That was 3 Kafka brokers (KRaft, RF=3), ZooKeeper, management, 2 frontends and 2
   consumer nodes, all unmodified 2.22.2 except a few assertions. Antithesis runs the cluster in a deterministic simulator and injects
   faults: network partitions, latency, node crashes, restarts and thread pauses.
3. **A small workload drove it** ([`workload/driver`](workload/driver/src/main.rs)). It publishes numbered messages to `ack=ALL`
   topics with SERIAL and BATCH subscriptions, flips subscriber endpoints between ok, 500, slow and flaky, suspends and activates,
   and updates policies and topics. Then it stops the faults and checks:
   - *every message acknowledged with 201 is eventually delivered to every subscription*
   - *nothing is delivered more than 60s after a confirmed suspend*
   - *inside `BatchConsumer`: offsets are marked processed only after delivery, a non-retried 4xx or TTL expiry*
     ([patch](workload/hermes-assertions.patch))
4. **The failures came with the full history.** In the run-2 timeline that exposed bug 1, the consumer
   reinitialised at t=105.66s. At t=122.99s a batch that got a single 503 was marked processed with `consuming=false`. At the end, 11 messages
   were missing from `b0`, while `s0`, the SERIAL subscription on the same topic, had all of them. Because the simulation is
   deterministic, any of these timelines can be replayed exactly, including in a debugger.

**Cost:** each 24-compute-hour run finished in about **37 minutes of wall-clock time**. The final run (run 3) failed only on Hermes bugs:

| Property | Failing timelines |
|---|---|
| Batch offsets committed without a successful delivery | 85 |
| 201-acked message never delivered | 48 |
| Delivery more than 60s after a confirmed suspend | 38 |
| Delivery body is not well-formed JSON (bug 3) | 147 |
| Hermes recovers once faults stop | passed |

**Why ordinary tests miss this:** bug 1 needs a topic update to land while the batch consumer is running, and then a
*later*, unrelated subscriber failure. Either event alone is harmless, and the loss is silent. Antithesis explores
those interleavings systematically, under faults.

---

## What's in this repo

```
repro/        plain-Docker reproductions against unmodified 2.22.2 (no Antithesis needed)
  build-images.sh     builds hermes-*:upstream-2.22.2, plus hermes-consumers:fixed-2.22.2 (bug 1 fix)
                      hermes-consumers:fixed-empty-2.22.2 (bug 3 fix), hermes-*:fixed-zk-2.22.2 (bug 4 fix)
  repro.sh            bug 1        repro_suspend.sh   bug 2
  repro_empty.sh      bug 3        repro_startup.sh   bug 4
workload/     the Antithesis test
  setup.sh            clones hermes-2.22.2, applies the assertions, installs the workload
  hermes-assertions.patch   Antithesis SDK assertions in hermes-consumers (no behaviour change)
  driver/             Rust workload (publish / fault / suspend / update + properties)
  config/             docker-compose topology used in Antithesis
FINDINGS.md   full technical write-up with evidence
```

These bugs have not been reported upstream yet. We wanted to share them with the Hermes team first.
We'd be glad to walk through the timelines, help get a fix in, or run this workload against your CI builds so
regressions like #1891 or #1908 are caught before release.
Contact: [Andrei-Antithesis](https://www.linkedin.com/in/andrei-chesnoiu/) · [antithesis.com](https://antithesis.com)
