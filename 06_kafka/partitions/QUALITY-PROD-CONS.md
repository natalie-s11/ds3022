# Streaming Data // Producers & Consumers

## Traits of Quality Kafka Producers

- **Idempotence:** `enable.idempotence=true` prevents duplicates on retry.
- **Retry within limits:** set a sensible `delivery.timeout.ms` and backoff instead of retrying forever.
- **Reconnecting:** tune `reconnect.backoff.ms` / `reconnect.backoff.max.ms` and handle broker failover gracefully.
- **Backpressure:** size `buffer.memory` and `max.block.ms`, and alert when sends block.
- **Resume with a cursor:** persist the source offset (outbox pattern or transactions) so restarts resume without loss.
- **Stable keys:** consistent partition keys keep per-entity ordering.
- **Batching:** tune `linger.ms` and `batch.size`.
- **Compression:** use `zstd` or `lz4` compression to speed up data flow.
- **Appropriate TTL:** align topic `retention.ms` with consumer recovery windows, and add expiry headers for time-sensitive data. Consider making sure resumed sessions are not greater than your TTL.
- **Observability and clean shutdown:** track error rates, latency, and any downstream consumer lag you cause; call `flush()` and `close()` on exit, never within the flow of messages themselves.

## Traits of Quality Kafka Consumers

- **Commit after processing:** set `enable.auto.commit=false` and commit offsets only after work succeeds, so nothing is marked as done before it’s done.
- **Idempotent processing:** redelivery will happen, so dedupe by key, offset, or event ID so that reprocessing is harmless.
- **Resume with a cursor:** choose `auto.offset.reset` (`earliest` or `latest`) deliberately, and store offsets atomically with results when you need exactly-once.
- **Liveness:** size `max.poll.records` and `max.poll.interval.ms` so each batch finishes in time, and tune `session.timeout.ms` / `heartbeat.interval.ms` so the consumer isn’t evicted.
- **"Bad" message handling:** route repeatedly failing messages to a dead-letter topic instead of blocking the partition forever. Set a max number of tries.
- **Backpressure:** `pause()` partitions when downstream is slow and `resume()` when it recovers, rather than stalling the poll loop.
- **Preserve ordering:** if you parallelize, do it per partition or per key, never across a single key’s messages.
- **Appropriate TTL:** skip stale data, and make sure recovery happens well within `retention.ms` so offsets don’t fall off the log.
- **Observability and clean shutdown:** track per-partition lag, processing latency, and error rates. On exit, call `wakeup()`, commit final offsets, and `close()` so the group rebalances promptly.
