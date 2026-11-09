from quixstreams import Application
from collections import Counter
import json
import logging
import os
import time
import redis

"""
Consumes Bluesky events from the `bluesky-events` topic and keeps running counts per
commit operation (create/update/delete) and per collection (app.bsky.feed.like, ...) in Redis.

- Counts accumulate in memory and are written to Redis in batches (every BATCH_SIZE messages
  or BATCH_SECONDS seconds) using a single MULTI/EXEC pipeline per batch.
- Offsets are stored only after Redis accepts a batch: at-least-once delivery. A crash between
  the Redis write and the offset commit can double-count that one batch on restart.
- Counts carry over between runs (the consumer group resumes from its committed offset).
- On Ctrl+C the pending batch is flushed, offsets are committed, and all project keys are printed.
"""

TOPIC = "bluesky-events"
PREFIX = "bluesky"                         # Namespace for every Redis key this project writes
BATCH_SIZE = 1000                          # Flush after this many messages...
BATCH_SECONDS = 2                          # ...or after this many seconds, whichever comes first
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bluesky-consumer.log")

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bluesky-consumer")

# Connection is lazy; decode_responses returns str instead of bytes
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def flush(counts, consumer, last_msg):
    """Write batched counts to Redis atomically, then mark last_msg's offset as processed.
    Returns True on success; on failure counts are kept so the next flush retries them."""
    if last_msg is None:
        return True
    try:
        pipe = r.pipeline(transaction=True)
        for key, n in counts.items():
            pipe.incrby(key, n)
        pipe.execute()
    except redis.RedisError as e:
        log.error("Redis flush failed (%d keys pending): %s", len(counts), e)
        return False
    consumer.store_offsets(last_msg)       # Only advance the offset once Redis has the counts
    log.info("Flushed %d keys through offset %s", len(counts), last_msg.offset())
    counts.clear()
    return True


def print_counts():
    """Print this project's keys only (found with SCAN, which doesn't block Redis like KEYS), highest first."""
    for kind in ("op", "collection"):
        keys = list(r.scan_iter(match=f"{PREFIX}:{kind}:*", count=500))
        rows = sorted(zip(keys, r.mget(keys)), key=lambda kv: -int(kv[1])) if keys else []
        print(f"\n--- {kind} ---")
        for key, count in rows:
            print(f"Type: {key} | Count: {count}")


def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="INFO",
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        consumer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )

    counts = Counter()                     # Pending (unflushed) increments, keyed by Redis key
    batch_n, last_msg, last_flush = 0, None, time.monotonic()

    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC])
        try:
            while True:
                msg = consumer.poll(1)

                if msg is None:
                    log.debug("Waiting...")
                elif msg.error() is not None:
                    raise Exception(msg.error())
                else:
                    # Every polled message advances the offset, even one we skip as malformed
                    last_msg = msg
                    batch_n += 1
                    try:
                        commit = json.loads(msg.value())["commit"]
                        counts[f"{PREFIX}:op:{commit['operation']}"] += 1
                        counts[f"{PREFIX}:collection:{commit['collection']}"] += 1
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        log.warning("Skipping bad message at offset %s: %s", msg.offset(), e)

                # Flush on size, or on time (also covers idle polls so slow traffic still lands)
                if batch_n >= BATCH_SIZE or time.monotonic() - last_flush >= BATCH_SECONDS:
                    if flush(counts, consumer, last_msg):
                        last_msg = None
                    batch_n, last_flush = 0, time.monotonic()
        finally:
            # Flush the final partial batch before the consumer closes and commits offsets
            flush(counts, consumer, last_msg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("Consumer crashed")
        raise
    finally:
        try:
            print_counts()
        except redis.RedisError as e:
            print(f"Could not read counts from Redis: {e}")
        r.close()
