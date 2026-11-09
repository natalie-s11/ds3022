from quixstreams import Application
import json
import redis

def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["bluesky-events"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                # get out various keys from the value
                operation = value['commit']['operation']
                collection = value['commit']['collection']
                
                # Do something with the messages
                # store incremental counts in redis by operation
                # or collection

                consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # redis_client = redis.Redis(host="localhost", port=6379, db=0)
        # for each key in redis, print the key and value
        # for key in redis_client.keys():
        #     print(f"Type: {key.decode('utf8')} | Count: {redis_client.get(key).decode('utf8')}")
        # redis_client.close()
        pass
