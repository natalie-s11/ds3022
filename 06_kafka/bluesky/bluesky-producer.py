import websocket
import json
import os
import time
from quixstreams import Application
from quixstreams.models import TopicConfig

"""
This script demonstrates how to use a Kafka producer to stream real-time
events from Bluesky Jetstream to a Kafka topic.

NOTE that it does not parse/consume messages, it publishes/produces them to Kafka.
Kafka publishing is currently commented out; the script only reads the stream.

If the connection drops, it reconnects with exponential backoff and resumes from the
last event it saw, using Jetstream's `cursor` parameter (a unix timestamp in microseconds).
The cursor location is stored in memory. What will happen if the module crashes?

Enable Kafka publishing:
- Uncomment the Kafka setup block below (`KAFKA_BROKER`, `app`, `topic`, `producer`).
    The imports it needs are already at the top of the file.
- In `on_message`, replace `print(did)` with a produce step: serialize the event with
    `topic.serialize()` (DID as the key, the full event as the value), then pass the
    serialized key and value to `producer.produce()` along with the topic name.
- Uncomment `producer.flush()` in the `finally` block so buffered messages are sent on exit.
- Run the script, then confirm messages are arriving in the `bluesky-events` topic
      (Redpanda Console, or run `bluesky-consumer.py`).
- OPTIONAL: Implement a method to measure/count msgs/sec every 5 seconds and print to stdout 
      as the script runs.

      
Ways to make this more robust once Kafka publishing is enabled:
- Save the cursor to a small local file (periodically and on exit) and load it on startup, so a
  restart resumes where it left off instead of skipping ahead. Ignore a saved cursor that is
  older than the topic's retention. TEST the cursor by running your script for a couple of minutes,
  then force it to stop using Ctrl+C, wait 5-10 seconds, then start it back up. You should see a
  large number of messages in the next count of msgs/sec.
- Add a delivery callback (`on_delivery`) so you know which messages Kafka accepted or rejected.
- Turn on `enable.idempotence` (implies `acks=all`) so the producer's internal retries can't
  create duplicates or reorder messages. Consider `compression.type` and larger batches too.
- Set the Kafka message timestamp to the event time (`time_us`) instead of the time it was received.
- Only advance the cursor after an event is safely handed to the producer, not when it arrives.
- Catch `BufferError` from `produce()`: it means Kafka is slow or down and the local buffer is full.
- When a message fails or the buffer fills, stop reading the stream, flush, and replay from the
  earliest failed event's cursor. Jetstream can act as your replay log.
- Before reconnecting, check that Kafka is reachable, and exit with an error after a few failed tries.
- Always `flush()` the producer before exiting, and handle SIGTERM (e.g. `docker stop`) as well as Ctrl-C.
- Reconnect if the stream goes silent for too long, not just when the connection drops.
- Rotate between Jetstream servers (jetstream1/2, us-east/us-west) on reconnect.
- Expect duplicates (at-least-once delivery): replays and the reconnect rewind can resend events.
  How would a consumer detect and drop them?
"""

# Jetstream Bluesky collections (posts, likes, reposts, follows, blocks, profiles, etc.)
JETSTREAM_URL = "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.*"
CURSOR_REWIND_US = 2_000_000               # On reconnect, rewind 2s so no events are missed (may produce a few duplicates)
MAX_BACKOFF = 60                           # Max seconds to wait between reconnect attempts
TOPIC_RETENTION_MS = 3 * 60 * 60 * 1000    # 3 hours
CURSOR_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jetstream_cursor.txt")
CURSOR_SAVE_EVERY = 1000                   # Save the cursor to disk every N produced messages

cursor = None                              # time_us of the most recent event seen; used to resume after a disconnect
backoff = 1                                # Current reconnect delay in seconds
stop = False                               # Set on Ctrl-C to break out of the reconnect loop
msg_count = 0                              # Messages produced since the last rate report
last_report = time.time()                  # When the last msgs/sec line was printed
REPORT_INTERVAL = 5                        # Seconds between msgs/sec reports
since_save = 0                             # Messages produced since the cursor was last saved


def load_cursor():
    """Return the saved cursor, or None if missing, unreadable, or older than the topic's retention."""
    try:
        with open(CURSOR_FILE) as f:
            saved = int(f.read().strip())
    except (FileNotFoundError, ValueError) as e:
        print(f"No usable saved cursor ({type(e).__name__}); starting from live")
        return None
    age_ms = time.time() * 1000 - saved / 1000
    if age_ms > TOPIC_RETENTION_MS:
        print(f"Saved cursor is {age_ms / 3_600_000:.1f}h old (older than retention); ignoring it")
        return None
    print(f"Loaded cursor {saved} ({age_ms / 1000:.0f}s old)")
    return saved

def save_cursor():
    """Write the cursor atomically (temp file + rename) so a crash mid-write can't corrupt it."""
    if cursor is None:
        return
    try:
        tmp = CURSOR_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(cursor))
        os.replace(tmp, CURSOR_FILE)
    except OSError as e:
        print(f"Failed to save cursor: {e}")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
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


def on_open(ws):
    """Connection established: reset the backoff delay."""
    global backoff
    backoff = 1
    print(f"Connected{f' (resuming from cursor {cursor})' if cursor else ''}")

def on_message(ws, message):
    """Handle each Jetstream event: track cursor, skip non-commit events, key by DID, (optionally) produce to Kafka."""
    global cursor, msg_count, last_report, since_save
    try:
        data = json.loads(message)

        # Every event (including identity/account) carries time_us; record it as the resume point
        cursor = data.get("time_us", cursor)

        # Jetstream also sends 'identity' and 'account' events, which have no 'commit' field
        if data.get("kind") != "commit":
            return

        # The DID (decentralized identifier) uniquely identifies the acting user
        did = data["did"]

        # Serialize the event (DID as key, full event as JSON value) and hand it to the producer's buffer
        msg = topic.serialize(key=did, value=data)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value)

        # Count produced messages and print the rate every REPORT_INTERVAL seconds
        msg_count += 1

        # Persist the cursor every CURSOR_SAVE_EVERY messages so a restart can resume from here
        since_save += 1
        if since_save >= CURSOR_SAVE_EVERY:
            save_cursor()
            since_save = 0
        elapsed = time.time() - last_report
        if elapsed >= REPORT_INTERVAL:
            print(f"{msg_count / elapsed:.1f} msgs/sec")
            msg_count, last_report = 0, time.time()

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Bad message ({type(e).__name__}: {e})")
    except Exception as e:
        print(f"Unexpected error handling message: {e}")

def on_error(ws, error):
    """Report WebSocket-level errors. Ctrl-C arrives here too (websocket-client swallows it), so flag a stop."""
    global stop
    if isinstance(error, KeyboardInterrupt):
        stop = True
    else:
        print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed ({close_status_code}: {close_msg})")

# Reconnect loop: each pass opens a connection (resuming from the cursor if we have one)
# and runs until it drops; then wait with exponential backoff and try again.
cursor = load_cursor()                     # Resume from the last saved position, if recent enough
try:
    while not stop:
        url = JETSTREAM_URL + (f"&cursor={cursor - CURSOR_REWIND_US}" if cursor else "")
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
        print(f"Disconnected. Reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
except KeyboardInterrupt:
    pass
finally:
    print("\nStopped.")
    # Ensure we flush on exit, then save the final cursor
    producer.flush()
    save_cursor()
