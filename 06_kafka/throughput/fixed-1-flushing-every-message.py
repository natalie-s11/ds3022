"""
FIX for MISSTEP 1: Stop flushing after every message.

produce() only adds the message to librdkafka's local buffer, and a background
thread sends batches to the brokers. Instead of blocking on flush() for every
message, we:
  1. call poll(0) on each message. It's non-blocking and runs delivery callbacks.
  2. count failed deliveries in a delivery callback, so every message is accounted for
  3. flush() once at shutdown, so nothing left in the buffer is lost
Result: throughput is limited by batching instead of round trips, and lag stays near 0.
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

# Log to a local file (and the console) so runs can be reviewed afterwards
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("fixed-1-flushing-every-message.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# linger.ms: wait up to 5 ms to fill a batch (more messages per request)
# compression.type: smaller batches on the wire, so fewer bytes per ack
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4", "linger.ms": 5, "compression.type": "lz4"},
)
topic = app.topic(name="trades", value_serializer="json")

failed = 0  # deliveries the broker rejected (updated in the callback below)


def on_delivery(err, msg):
    """Runs from poll()/flush() once the broker acks or rejects a message."""
    global failed
    if err is not None:
        failed += 1
        log.error(f"Delivery failed for key={msg.key()}: {err}")


async def main():
    """Read trades from the WebSocket and produce them to Kafka without per-message flushes."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:
        try:
            async with websockets.connect(URL, close_timeout=1) as ws:  # don't wait 10 s for a close handshake on a dead connection
                async for message in ws:
                    data = json.loads(message)
                    msg = topic.serialize(key=data["symbol"], value=data)

                    # Non-blocking enqueue. If librdkafka's buffer is full, quixstreams
                    # polls and retries here. This is where backpressure applies.
                    producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
                    producer.poll(0)  # <-- THE FIX: run delivery callbacks, never wait on the network

                    # Bookkeeping: count gaps in `seq` and how far behind real time we are
                    if last_seq is not None and data["seq"] != last_seq + 1:
                        missed += data["seq"] - last_seq - 1
                    last_seq, count = data["seq"], count + 1
                    if count % 1000 == 0:
                        log.info(f"produced={count} missed={missed} failed={failed} lag={time.time() - data['ts']:.2f}s")
        except websockets.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e!r}")
        except Exception:
            log.exception("Unexpected error in producer loop")
        finally:
            # Flush once at shutdown, so messages still in the buffer reach Kafka
            remaining = producer.flush(timeout=10)
            if remaining:
                log.error(f"{remaining} messages not delivered before shutdown")
            log.info(f"Total produced: {count}, missed: {missed}, failed: {failed}")


try:
    asyncio.run(main())
except KeyboardInterrupt:                               # Ctrl+C: shutdown already drained and flushed in main()'s finally
    log.info("Stopped by user (Ctrl+C)")
