# Reproduction: batch subscription loses acknowledged messages after a topic update

Plain Docker, no Antithesis. Runs against **unmodified `hermes-2.22.2`**.

The script runs these steps:
1. Start Hermes (3 Kafka brokers in KRaft mode, ZooKeeper, management, frontend, consumers) with `consumer.useTopicMessageSizeEnabled: true`.
2. Create an `ack=ALL` JSON topic with a BATCH subscription (`batchSize: 1`, `messageTtl: 3600`).
3. `PUT /topics/repro.t {"maxMessageSize": 20480}`, then wait for the consumer log line
   `Reinitializing message receiver, contentType or messageSize changed.`
4. Make the subscriber return 503, and publish ids 101–103. Each one gets `201 Created`.
5. Make the subscriber healthy again, publish 201–203, and wait 60s.

Result on 2.22.2:

    delivery attempts per batch (mode, ids): [('fail', [101]), ('fail', [102]), ('fail', [103])]
    delivered: [201, 202, 203]
    LOST (acked 201, never delivered, never retried): [101, 102, 103]

Each failed batch is attempted once, never retried, and never logged as discarded, and its offset is committed.
With `fix-batchconsumer-consuming-flag.diff` (set `consuming = true` in `BatchConsumer.initialize()`),
the same script keeps retrying every 200ms (about 20 failed attempts in our runs, depending on timing) and
delivers all six messages once the subscriber recovers.

Note: `consumer.useTopicMessageSizeEnabled` is `false` by default. The images enable it in
`workload/sut/consumers.yaml` so that a `maxMessageSize` update reinitialises the receiver. The Hermes code itself
is unmodified.

## Run it

    repro/build-images.sh                               # clones hermes-2.22.2, builds plain images (+ fixed images for bugs 1, 3 and 4)
    repro/repro.sh                                      # BUG REPRODUCED
    CONSUMERS_TAG=fixed-2.22.2 repro/repro.sh           # one-line fix applied: no loss
    repro/repro_suspend.sh                              # bug 2: deliveries after suspend (watches for 6 minutes)
    repro/repro_empty.sh                                # bug 3: empty-body POST after a topic update
    CONSUMERS_TAG=fixed-empty-2.22.2 repro/repro_empty.sh   # 4-line fix applied: no empty POST
    repro/repro_startup.sh 10                           # bug 4: crash when nodes start together (10 fresh starts)
    HERMES_TAG=fixed-zk-2.22.2 repro/repro_startup.sh 10    # 3-line fix applied: no crash

## Root cause

`BatchConsumer.tearDown()` sets `consuming = false`. `updateTopic()` reinitialises with
`tearDown(); initialize();`, and `initialize()` never sets the flag back. The retryer's
`retryIfResult(result -> consuming && !result.succeeded() && …)` is then false for the life of that
consumer: the first failed attempt is returned as the final result, `deliver()` returns normally, and
`consume()` calls `offerProcessedOffsets(batch)`.
