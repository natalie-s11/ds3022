"""
FIX for MISSTEP 5: Resume from a cursor on reconnect, instead of jumping to live.

The original reconnected to the live end of the stream, so every drop skipped the
~1000+ messages sent while we were away. `lag` looked healthy (~0) while `missed`
grew. The fix:
  1. Resume: reconnect with ?cursor=<last_seq + 1>, and the server replays from its
     60 s ring buffer, so there are no gaps.
  2. Dedupe and detect: skip seq <= last_seq (overlapping replay). A gap means we
     were away longer than the 60 s window (the server clamps the cursor), so that
     data is truly lost: log it loudly.
  3. Back off: exponential backoff with jitter, capped well under the replay window,
     and reset after a successful connection. Only reconnect on CONNECTION errors;
     real bugs crash instead of looping forever.
  4. Absorb the burst: a resume replays a backlog as fast as the socket allows. With
     no per-message flush (fix 1), produce() absorbs it, and `lag` spikes, then falls
     back to ~0 as we catch up.
Limitation: the cursor lives in memory only. A process restart starts live again.
Persist last_seq (to a file, or read it back from Kafka) to survive restarts.
"""
import asyncio
import json
import logging
import os
import random
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
DROP_CHANCE = 0.0005                                    # per message: a simulated drop every ~2 s (kept for the demo)
BACKOFF_START, BACKOFF_MAX = 0.1, 2.0                   # seconds; the cap stays far below the server's 60 s window

# Log to a local file (and the console) so runs can be reviewed afterwards
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("fixed-5-reconnecting-without-a-resume-cursor.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4", "linger.ms": 5, "compression.type": "lz4"},
)
topic = app.topic(name="trades", value_serializer="json")

stats = {"produced": 0, "missed": 0, "dupes": 0, "failed": 0, "reconnects": 0}


def on_delivery(err, msg):
    """Runs from poll()/flush() once the broker acks or rejects a message."""
    if err is not None:
        stats["failed"] += 1
        log.error(f"Delivery failed for key={msg.key()}: {err}")


async def main():
    """Produce trades to Kafka, resuming from the last seq after every dropped connection."""
    last_seq, backoff = None, BACKOFF_START
    with app.get_producer() as producer:                # one producer for all connections: buffered messages survive a drop
        try:
            while True:
                # <-- THE FIX: resume from where we left off (live only on the very first connect)
                url = URL if last_seq is None else f"{URL}?cursor={last_seq + 1}"
                try:
                    async with websockets.connect(url, close_timeout=1) as ws:  # don't wait 10 s for a close handshake on a dead connection
                        log.info(f"Connected: {url}")
                        backoff = BACKOFF_START          # reset backoff once a connection succeeds
                        async for message in ws:
                            data = json.loads(message)
                            seq = data["seq"]

                            if last_seq is not None:
                                if seq <= last_seq:      # replay overlap: already produced
                                    stats["dupes"] += 1
                                    continue
                                if seq != last_seq + 1:  # gap: we were away longer than the replay window
                                    stats["missed"] += seq - last_seq - 1
                                    log.warning(f"DATA LOST: seq {last_seq + 1}..{seq - 1} fell out of the replay window")

                            msg = topic.serialize(key=data["symbol"], value=data)
                            producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
                            producer.poll(0)             # run delivery callbacks without blocking
                            last_seq = seq               # advance the cursor only after the message is handed to Kafka
                            stats["produced"] += 1

                            if stats["produced"] % 1000 == 0:
                                log.info(f"produced={stats['produced']} missed={stats['missed']} dupes={stats['dupes']} "
                                         f"failed={stats['failed']} reconnects={stats['reconnects']} "
                                         f"lag={time.time() - data['ts']:.2f}s")

                            if random.random() < DROP_CHANCE:
                                raise ConnectionError("simulated network drop")
                except (websockets.ConnectionClosed, OSError) as e:  # ConnectionError is an OSError
                    stats["reconnects"] += 1
                    delay = backoff * random.uniform(0.5, 1.5)        # jitter avoids synchronized reconnect storms
                    log.warning(f"Connection lost: {e!r}; resuming at seq {None if last_seq is None else last_seq + 1} "
                                f"in {delay:.2f} s")
                    await asyncio.sleep(delay)
                    backoff = min(backoff * 2, BACKOFF_MAX)
        except Exception:
            log.exception("Unexpected error; stopping (not a connection problem, so retrying won't help)")
        finally:
            remaining = producer.flush(timeout=10)       # flush once at shutdown (also on Ctrl+C)
            if remaining:
                log.error(f"{remaining} messages not delivered before shutdown")
            log.info(f"Total produced: {stats['produced']}, missed: {stats['missed']}, "
                     f"dupes: {stats['dupes']}, reconnects: {stats['reconnects']}")


try:
    asyncio.run(main())
except KeyboardInterrupt:                               # Ctrl+C: shutdown already drained and flushed in main()'s finally
    log.info("Stopped by user (Ctrl+C)")
