"""
FIX for MISSTEP 3: Bound the queue and speed up the writer.

The original used an unbounded asyncio.Queue between a fast receiver and a single
~500 msg/s writer. put() never waited, so the backlog grew in OUR memory, where the
server can't see it or replay it, until the process crashed and lost it all.
The fix has three parts:
  1. Bounded queue (maxsize): when it's full, `await queue.put()` waits, so the
     receiver stops reading and the backlog stays on the server, inside its replay
     window. maxsize also caps the latency the queue adds (~maxsize / throughput).
  2. Faster writer: a fixed pool of NUM_WRITERS. 1000 msg/s * 2 ms needs at least 2
     in flight (Little's Law), and 8 gives ~4x headroom.
  3. Drain on shutdown: wait for the queue to empty and flush the producer, so
     messages already received still reach Kafka.
Trade-off: parallel writers can reorder messages for the same symbol. See
fixed-2-blocking-the-event-loop.py for an ordered (sharded by symbol) version.
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
QUEUE_MAX = 1000      # <-- THE FIX: bounded (~1 s of traffic); replaces the simulated MEMORY_LIMIT
NUM_WRITERS = 8       # concurrent writers; needs to be >= rate * latency (1000 * 0.002 = 2)
DRAIN_TIMEOUT = 10    # seconds to finish queued work on shutdown

# Log to a local file (and the console) so runs can be reviewed afterwards
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("fixed-3-unbounded-asyncio-queue.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4", "linger.ms": 5, "compression.type": "lz4"},
)
topic = app.topic(name="trades", value_serializer="json")

stats = {"produced": 0, "missed": 0, "failed": 0}


def on_delivery(err, msg):
    """Runs from poll()/flush() once the broker acks or rejects a message."""
    if err is not None:
        stats["failed"] += 1
        log.error(f"Delivery failed for key={msg.key()}: {err}")


async def receiver(queue):
    """Read the socket, check `seq` for gaps, and enqueue. put() waits when the queue is full."""
    last_seq = None
    async with websockets.connect(URL, close_timeout=1) as ws:  # don't wait 10 s for a close handshake on a dead connection
        async for message in ws:
            data = json.loads(message)
            if last_seq is not None and data["seq"] != last_seq + 1:
                stats["missed"] += data["seq"] - last_seq - 1
            last_seq = data["seq"]
            await queue.put(data)                       # backpressure: blocks here instead of growing memory


async def writer(queue, producer):
    """Pull from the queue, do the slow downstream work, and produce to Kafka."""
    while True:
        data = await queue.get()
        try:
            await asyncio.sleep(0.002)                  # slow downstream work (~500 msg/s per writer)
            msg = topic.serialize(key=data["symbol"], value=data)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
            producer.poll(0)                            # run delivery callbacks without blocking
            stats["produced"] += 1
            if stats["produced"] % 1000 == 0:
                log.info(f"produced={stats['produced']} missed={stats['missed']} failed={stats['failed']} "
                         f"queued={queue.qsize():,} lag={time.time() - data['ts']:.2f}s")
        except Exception:
            log.exception(f"Failed to process seq={data.get('seq')}")  # one bad message doesn't kill the writer
        finally:
            queue.task_done()


async def main():
    """Run the receiver and a writer pool over a bounded queue, then drain and flush on exit."""
    queue = asyncio.Queue(maxsize=QUEUE_MAX)
    with app.get_producer() as producer:
        writers = [asyncio.create_task(writer(queue, producer)) for _ in range(NUM_WRITERS)]
        try:
            await receiver(queue)
        except websockets.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e!r}")
        except Exception:
            log.exception("Unexpected error in receiver")
        finally:
            # Drain what's already queued instead of losing it (the original lost everything here)
            try:
                await asyncio.wait_for(queue.join(), timeout=DRAIN_TIMEOUT)
            except asyncio.TimeoutError:
                log.error(f"Drain timed out; {queue.qsize():,} queued messages not produced")
            for w in writers:
                w.cancel()
            remaining = producer.flush(timeout=10)      # flush once at shutdown
            if remaining:
                log.error(f"{remaining} messages not delivered before shutdown")
            log.info(f"Total produced: {stats['produced']}, missed: {stats['missed']}, failed: {stats['failed']}")


try:
    asyncio.run(main())
except KeyboardInterrupt:                               # Ctrl+C: shutdown already drained and flushed in main()'s finally
    log.info("Stopped by user (Ctrl+C)")
