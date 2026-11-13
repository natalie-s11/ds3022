"""
FIX for MISSTEP 4: Limit the work in flight BEFORE starting each task.

The original called create_task() for every message, and each task then waited
for one of the downstream service's 10 slots. create_task() never waits, so
waiting tasks piled up in memory (~500/s) until the process crashed and lost them.
The fix:
  1. Acquire an in-flight slot in the READER, before create_task(). Once
     MAX_IN_FLIGHT tasks are running, the reader waits, so the socket backs up and
     the backlog stays on the server, inside its replay window. Memory stays flat.
  2. Each task releases its slot in `finally`, even when it fails.
  3. Drain the in-flight tasks and flush the producer on shutdown.
The ceiling: the service handles 10 concurrent requests * 20 ms = 500 req/s (Little's
Law). No client-side control flow can push more than that through it, so against
1000 msg/s lag still grows. This fix makes the failure SAFE, with no memory blow-up
and nothing lost from our process. To actually keep up, make fewer downstream calls
(cache, batch) or get more capacity from the service.
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
MAX_IN_FLIGHT = 20    # <-- THE FIX: cap on tasks in flight (a bit above downstream's 10 slots, so it stays busy)
DRAIN_TIMEOUT = 10    # seconds to finish in-flight tasks on shutdown

# Log to a local file (and the console) so runs can be reviewed afterwards
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("fixed-4-task-per-message.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4", "linger.ms": 5, "compression.type": "lz4"},
)
topic = app.topic(name="trades", value_serializer="json")
downstream = asyncio.Semaphore(10)                      # the enrichment SERVICE's limit (not ours to change)
inflight = asyncio.Semaphore(MAX_IN_FLIGHT)             # OUR limit, acquired by the reader before create_task
pending = set()                                         # strong refs to running tasks (also used to drain on exit)
stats = {"produced": 0, "missed": 0, "failed": 0, "lag": 0.0}


def on_delivery(err, msg):
    """Runs from poll()/flush() once the broker acks or rejects a message."""
    if err is not None:
        stats["failed"] += 1
        log.error(f"Delivery failed for key={msg.key()}: {err}")


async def handle(producer, data):
    """Enrich one trade through the downstream service, produce it, and always release its slot."""
    try:
        async with downstream:
            await asyncio.sleep(0.02)                   # simulated 20 ms async HTTP call
        data["enriched"] = True
        msg = topic.serialize(key=data["symbol"], value=data)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
        producer.poll(0)                                # run delivery callbacks without blocking
        stats["produced"] += 1
        stats["lag"] = time.time() - data["ts"]
    except Exception:
        log.exception(f"Failed to process seq={data.get('seq')}")
    finally:
        inflight.release()                              # free the slot so the reader can start another task


async def reader(ws, producer):
    """Read trades, check `seq` for gaps, and start a task only once an in-flight slot is free."""
    count, last_seq = 0, None
    async for message in ws:
        data = json.loads(message)
        if last_seq is not None and data["seq"] != last_seq + 1:
            stats["missed"] += data["seq"] - last_seq - 1
        last_seq = data["seq"]

        await inflight.acquire()                        # backpressure: waits here when MAX_IN_FLIGHT are running
        task = asyncio.create_task(handle(producer, data))
        pending.add(task)
        task.add_done_callback(pending.discard)

        count += 1
        if count % 1000 == 0:
            log.info(f"received={count} produced={stats['produced']} missed={stats['missed']} "
                     f"failed={stats['failed']} in_flight={len(pending)} lag={stats['lag']:.2f}s")


async def main():
    """Run the reader with bounded in-flight work, then drain the tasks and flush on exit."""
    with app.get_producer() as producer:
        try:
            async with websockets.connect(URL, close_timeout=1) as ws:  # don't wait 10 s for a close handshake on a dead connection
                await reader(ws, producer)
        except websockets.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e!r}")
        except Exception:
            log.exception("Unexpected error in reader")
        finally:
            # Let in-flight tasks finish instead of cancelling them (the original lost them all here)
            if pending:
                done, not_done = await asyncio.wait(pending, timeout=DRAIN_TIMEOUT)
                for task in not_done:
                    task.cancel()
                if not_done:
                    log.error(f"Drain timed out; {len(not_done)} in-flight messages not produced")
            remaining = producer.flush(timeout=10)      # flush once at shutdown
            if remaining:
                log.error(f"{remaining} messages not delivered before shutdown")
            log.info(f"Total produced: {stats['produced']}, missed: {stats['missed']}, failed: {stats['failed']}")


try:
    asyncio.run(main())
except KeyboardInterrupt:                               # Ctrl+C: shutdown already drained and flushed in main()'s finally
    log.info("Stopped by user (Ctrl+C)")
