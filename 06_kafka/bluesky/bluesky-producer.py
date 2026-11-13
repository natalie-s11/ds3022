import websocket
import json
import os
import sys
import time
import signal
import logging
import threading
from itertools import cycle
from quixstreams import Application
from quixstreams.models import TopicConfig

"""
This script demonstrates how to use a Kafka producer to stream real-time
events from Bluesky Jetstream to a Kafka topic.

NOTE that it does not parse/consume messages, it publishes/produces them to Kafka.

If the connection drops, it reconnects with exponential backoff and resumes from the
last event it saw, using Jetstream's `cursor` parameter (a unix timestamp in microseconds).
The cursor is saved to a local file (periodically and on exit) so a restart resumes too.

Robustness features:
- Delivery callback (`on_delivery`) records any message Kafka rejects.
- Idempotent producer (`acks=all`) with lz4 compression and batching.
- Cursor only advances after `produce()` accepts the event; `BufferError` is caught.
- On a failed delivery or full buffer: stop reading, flush, and replay from the earliest
  failed event's cursor (Jetstream is the replay log).
- Before each (re)connect, checks Kafka is reachable; exits after 3 failed tries.
- Reconnects if the stream goes silent for 30s, rotating between 4 Jetstream servers.
- SIGTERM (e.g. `docker stop`) is handled like Ctrl-C: flush and save the cursor.
- Kafka message timestamps are set to the event time (`time_us`).
- Expect duplicates (at-least-once delivery): replays and the reconnect rewind can resend events.
  How would a consumer detect and drop them?
"""

# Jetstream servers to rotate between on reconnect (all serve Bluesky collections: posts, likes, etc.)
JETSTREAM_HOSTS = cycle([
    "jetstream2.us-west.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
])
CURSOR_REWIND_US = 2_000_000               # On reconnect, rewind 2s so no events are missed (may produce a few duplicates)
MAX_BACKOFF = 60                           # Max seconds to wait between reconnect attempts
TOPIC_RETENTION_MS = 3 * 60 * 60 * 1000    # 3 hours
HERE = os.path.dirname(os.path.abspath(__file__))
CURSOR_FILE = os.path.join(HERE, "jetstream_cursor.txt")
CURSOR_SAVE_EVERY = 1000                   # Save the cursor to disk every N produced messages
REPORT_INTERVAL = 5                        # Seconds between msgs/sec reports
STALL_TIMEOUT = 30                         # Reconnect if no message arrives for this many seconds
KAFKA_MAX_TRIES = 3                        # Failed Kafka reachability checks before exiting

cursor = None                              # time_us of the last event safely handed to the producer
backoff = 1                                # Current reconnect delay in seconds
stop = False                               # Set on Ctrl-C/SIGTERM to break out of the reconnect loop
msg_count = 0                              # Messages produced since the last rate report
last_report = time.time()                  # When the last msgs/sec line was printed
since_save = 0                             # Messages produced since the cursor was last saved
last_msg = time.time()                     # When the last message arrived (for stall detection)
failed_cursor = None                       # Earliest time_us that Kafka failed to accept; triggers a replay
ws = None                                  # Current WebSocket connection (closed by watchdog/signal handler)

# Log to a local file and to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(os.path.join(HERE, "bluesky-producer.log")), logging.StreamHandler()],
)
log = logging.getLogger("bluesky-producer")


def load_cursor():
    """Return the saved cursor, or None if missing, unreadable, or older than the topic's retention."""
    try:
        with open(CURSOR_FILE) as f:
            saved = int(f.read().strip())
    except (FileNotFoundError, ValueError) as e:
        log.info(f"No usable saved cursor ({type(e).__name__}); starting from live")
        return None
    age_ms = time.time() * 1000 - saved / 1000
    if age_ms > TOPIC_RETENTION_MS:
        log.info(f"Saved cursor is {age_ms / 3_600_000:.1f}h old (older than retention); ignoring it")
        return None
    log.info(f"Loaded cursor {saved} ({age_ms / 1000:.0f}s old)")
    return saved

def save_cursor():
    """Write the cursor atomically (temp file + rename) so a crash mid-write can't corrupt it.
    Never saves past an event that failed delivery."""
    resume = min(c for c in (cursor, failed_cursor) if c) if (cursor or failed_cursor) else None
    if resume is None:
        return
    try:
        tmp = CURSOR_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(resume))
        os.replace(tmp, CURSOR_FILE)
    except OSError as e:
        log.error(f"Failed to save cursor: {e}")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        # Idempotence (requires acks=all): internal retries can't duplicate or reorder messages
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",  # Fast, low-CPU compression
        # Batch settings for better throughput
        "linger.ms": 5,  # Wait up to 5ms to batch messages
        "batch.size": 100000,  # Batch up to 100KB
    }
)
topic = app.topic(
    name="bluesky-events",
    value_serializer="json",
    config=TopicConfig(
        num_partitions=1,
        replication_factor=3,
        extra_config={"retention.ms": str(TOPIC_RETENTION_MS)}
    )
)

# Create a kafka producer, reused for every message
producer = app.get_producer()


def kafka_reachable():
    """Return True if the brokers answer a metadata request within 5s."""
    try:
        producer._producer.list_topics(timeout=5)  # underlying confluent_kafka producer
        return True
    except Exception as e:
        log.warning(f"Kafka not reachable: {e}")
        return False

def request_replay(time_us):
    """Remember the earliest event Kafka didn't accept and close the stream so we can replay from it."""
    global failed_cursor
    failed_cursor = time_us if failed_cursor is None else min(failed_cursor, time_us)
    if ws:
        ws.close()

def make_delivery_cb(time_us):
    """Build a delivery callback that knows which event (by time_us) it belongs to."""
    def on_delivery(err, msg):
        if err is not None:
            log.error(f"Delivery failed (time_us={time_us}): {err}")
            request_replay(time_us)
    return on_delivery

def watchdog():
    """Background thread: close the connection if the stream goes silent for STALL_TIMEOUT seconds."""
    while not stop:
        time.sleep(5)
        if ws and time.time() - last_msg > STALL_TIMEOUT:
            log.warning(f"No messages for {STALL_TIMEOUT}s; reconnecting")
            ws.close()

def handle_sigterm(signum, frame):
    """Treat SIGTERM (e.g. `docker stop`) like Ctrl-C: stop the loop so `finally` flushes and saves."""
    global stop
    log.info("SIGTERM received; shutting down")
    stop = True
    if ws:
        ws.close()


def on_open(ws):
    """Connection established: reset the backoff delay and the stall timer."""
    global backoff, last_msg
    backoff, last_msg = 1, time.time()
    log.info(f"Connected{f' (resuming from cursor {cursor})' if cursor else ''}")

def on_message(ws, message):
    """Handle each Jetstream event: skip non-commit events, key by DID, produce to Kafka, then advance the cursor."""
    global cursor, msg_count, last_report, since_save, last_msg
    last_msg = time.time()
    if failed_cursor is not None:
        return  # A replay is pending; ignore anything still arriving on this connection
    try:
        data = json.loads(message)
        time_us = data.get("time_us")

        # Jetstream also sends 'identity' and 'account' events, which have no 'commit' field
        if data.get("kind") != "commit":
            cursor = time_us or cursor
            return

        # The DID (decentralized identifier) uniquely identifies the acting user
        did = data["did"]

        # Serialize the event (DID as key, full event as JSON value) and hand it to the producer's buffer,
        # stamped with the event time (ms) rather than the time we received it
        msg = topic.serialize(key=did, value=data)
        try:
            producer.produce(topic=topic.name, key=msg.key, value=msg.value,
                             timestamp=time_us // 1000, on_delivery=make_delivery_cb(time_us))
        except BufferError:
            log.error("Producer buffer full (Kafka slow or down); replaying")
            request_replay(time_us)
            return

        # Only now is the event safely with the producer, so advance the cursor
        cursor = time_us
        msg_count += 1

        # Persist the cursor every CURSOR_SAVE_EVERY messages so a restart can resume from here
        since_save += 1
        if since_save >= CURSOR_SAVE_EVERY:
            save_cursor()
            since_save = 0

        # Print the rate every REPORT_INTERVAL seconds
        elapsed = time.time() - last_report
        if elapsed >= REPORT_INTERVAL:
            log.info(f"{msg_count / elapsed:.1f} msgs/sec")
            msg_count, last_report = 0, time.time()

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log.warning(f"Bad message ({type(e).__name__}: {e})")
    except Exception as e:
        log.error(f"Unexpected error handling message: {e}")

def on_error(ws, error):
    """Report WebSocket-level errors. Ctrl-C arrives here too (websocket-client swallows it), so flag a stop."""
    global stop
    if isinstance(error, KeyboardInterrupt):
        stop = True
    else:
        log.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    log.info(f"WebSocket closed ({close_status_code}: {close_msg})")


# Reconnect loop: each pass checks Kafka, opens a connection (resuming from the cursor if we have one)
# and runs until it drops; then replays failures or waits with exponential backoff and tries again.
signal.signal(signal.SIGTERM, handle_sigterm)
threading.Thread(target=watchdog, daemon=True).start()
cursor = load_cursor()                     # Resume from the last saved position, if recent enough
try:
    while not stop:
        # Make sure Kafka is up before reading the stream; give up after KAFKA_MAX_TRIES
        for attempt in range(1, KAFKA_MAX_TRIES + 1):
            if kafka_reachable():
                break
            if attempt == KAFKA_MAX_TRIES:
                log.critical(f"Kafka unreachable after {KAFKA_MAX_TRIES} tries; exiting")
                sys.exit(1)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

        url = (f"wss://{next(JETSTREAM_HOSTS)}/subscribe?wantedCollections=app.bsky.*"
               + (f"&cursor={cursor - CURSOR_REWIND_US}" if cursor else ""))
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        # Pings detect silently dead connections; reconnect=0 hands reconnection to this loop
        ws.run_forever(ping_interval=30, ping_timeout=10, reconnect=0)
        if stop:
            break

        # A delivery failed or the buffer filled: flush what's pending, then rewind to the earliest failure
        if failed_cursor is not None:
            producer.flush(timeout=30)
            cursor, failed_cursor = failed_cursor, None
            log.info(f"Replaying from cursor {cursor}")
            continue
        log.info(f"Disconnected. Reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
except KeyboardInterrupt:
    pass
finally:
    log.info("Stopped.")
    # Ensure we flush on exit (bounded so a dead Kafka can't hang shutdown), then save the final cursor
    producer.flush(timeout=30)
    save_cursor()
