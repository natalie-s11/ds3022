#!/usr/bin/env python3

"""
Reads puzzle messages from the "puzzle" topic and rebuilds the phrase
by sorting the letters on order_no.
"""

import os
import json
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "puzzle"
MAX_EMPTY_POLLS = 5

# Kafka app; start from the oldest message. Uncomment extra_config to force IPv4 if using "localhost"
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="puzzle-consumer",
    auto_offset_reset="earliest",
    # consumer_extra_config={"broker.address.family": "v4"},
)

# Collect letters until we have all `total` of them, then print the phrase
def consume_messages():
    letters = {}  # order_no -> letter
    try:
        # No offset commits, so every run re-reads the topic from the start
        with app.get_consumer(auto_commit_enable=False) as consumer:
            consumer.subscribe([TOPIC])
            print(f"Consuming from topic '{TOPIC}' on {KAFKA_BROKER}")
            empty_polls = 0
            while empty_polls < MAX_EMPTY_POLLS:
                msg = consumer.poll(1)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                empty_polls = 0
                value = json.loads(msg.value())
                letters[value["order_no"]] = value["letter"]
                # Stop once every letter has arrived
                if len(letters) == value["total"]:
                    break
        # Sort by order_no and join the letters
        phrase = "".join(letters[i] for i in sorted(letters))
        print(f"Phrase: {phrase}")
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

if __name__ == "__main__":
    consume_messages()
