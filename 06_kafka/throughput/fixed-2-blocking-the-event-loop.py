"""
FIX for MISSTEP 2: non-blocking, concurrent, AND ordered per symbol.

The original ran a 5 ms synchronous lookup inline, which froze the only thread and
capped us at 1 / 0.005 = 200 msg/s. Here enrich() is async (the loop keeps reading
and answering pings while a lookup waits), and a reader feeds a fixed worker pool
through bounded queues. Each worker has its OWN queue, and every message for a
symbol goes to the same worker. (One shared queue would let parallel workers
reorder a symbol's trades.)
  - Ordering: one worker per symbol handles its messages in arrival order, so
    Kafka receives (and consumers see) each symbol's trades in order.
  - Parallelism: different symbols run concurrently. With 10 symbols at ~100 msg/s
    each and a 5 ms lookup (200 msg/s per worker), there's about 2x headroom.
  - Shard assignment: round-robin as each new symbol first appears (not hash % N),
    so while #symbols <= NUM_WORKERS no two symbols share a worker.
Backpressure: if any shard's queue fills, `await put()` waits and the reader
slows down for everyone. That is head-of-line blocking, but memory stays bounded.
Limit: a single hot symbol can never go faster than one worker (200 msg/s here).
"""
import asyncio
import json
import logging
import os
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
NUM_WORKERS = 10      # one per symbol in the streamer; extra workers beyond #symbols sit idle
SHARD_QUEUE_MAX = 100 # per-shard buffer (~1 s of one symbol's traffic) before the reader waits

# Log to a local file (and the console) so runs can be reviewed afterwards
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("fixed-2-blocking-the-event-loop.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4", "linger.ms": 5, "compression.type": "lz4"},
)
topic = app.topic(name="trades", value_serializer="json")

stats = {"produced": 0, "missed": 0, "failed": 0}


async def enrich(data):
    """Async stand-in for a lookup, e.g. `await client.get(...)` with httpx.AsyncClient.
    For a sync-only library use: `await asyncio.to_thread(sync_lookup, data)`."""
    await asyncio.sleep(0.005)                          # <-- THE FIX (yields to the loop instead of freezing it)
    data["enriched"] = True
    return data


def on_delivery(err, msg):
    """Runs from poll()/flush() once the broker acks or rejects a message."""
    if err is not None:
        stats["failed"] += 1
        log.error(f"Delivery failed for key={msg.key()}: {err}")


async def reader(ws, queues):
    """Read the socket, check `seq` for gaps, and route each message to its symbol's shard."""
    last_seq, shard_of = None, {}                       # shard_of: symbol -> worker index
    async for message in ws:
        data = json.loads(message)
        if last_seq is not None and data["seq"] != last_seq + 1:
            stats["missed"] += data["seq"] - last_seq - 1
        last_seq = data["seq"]
        # Same symbol -> same queue -> same worker -> order preserved
        idx = shard_of.setdefault(data["symbol"], len(shard_of) % NUM_WORKERS)
        await queues[idx].put(data)                     # waits when this shard is full: backpressure


async def worker(queue, producer, queues):
    """Process one shard's messages one at a time (in order), producing each to Kafka."""
    while True:
        data = await queue.get()
        try:
            data = await enrich(data)
            msg = topic.serialize(key=data["symbol"], value=data)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
            producer.poll(0)                            # run delivery callbacks without blocking
            stats["produced"] += 1
            if stats["produced"] % 1000 == 0:
                log.info(f"produced={stats['produced']} missed={stats['missed']} failed={stats['failed']} "
                         f"queued={[q.qsize() for q in queues]} lag={time.time() - data['ts']:.2f}s")
        except Exception:
            log.exception(f"Failed to process seq={data.get('seq')}")  # one bad message doesn't kill the worker
        finally:
            queue.task_done()


async def main():
    """Start one worker per shard, run the reader, then drain the shards and flush on shutdown."""
    queues = [asyncio.Queue(maxsize=SHARD_QUEUE_MAX) for _ in range(NUM_WORKERS)]
    with app.get_producer() as producer:
        workers = [asyncio.create_task(worker(q, producer, queues)) for q in queues]
        try:
            async with websockets.connect(URL, close_timeout=1) as ws:  # don't wait 10 s for a close handshake on a dead connection
                await reader(ws, queues)
        except websockets.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e!r}")
        except Exception:
            log.exception("Unexpected error in reader")
        finally:
            await asyncio.gather(*(q.join() for q in queues))  # let every shard finish what's queued
            for w in workers:
                w.cancel()
            remaining = producer.flush(timeout=10)      # flush once at shutdown
            if remaining:
                log.error(f"{remaining} messages not delivered before shutdown")
            log.info(f"Total produced: {stats['produced']}, missed: {stats['missed']}, failed: {stats['failed']}")


try:
    asyncio.run(main())
except KeyboardInterrupt:                               # Ctrl+C: shutdown already drained and flushed in main()'s finally
    log.info("Stopped by user (Ctrl+C)")
