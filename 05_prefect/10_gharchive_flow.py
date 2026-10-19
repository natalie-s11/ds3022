import gzip
import json
import logging
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"

# log to a local file alongside Prefect's own console logging
logging.basicConfig(filename="gharchive_flow.log", level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@task(retries=3)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    # use a Counter named "counts" and pull out the "type"
    # for each line in the stream
    counts = Counter()
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
            r.raise_for_status()
            # decompress the gzip stream on the fly; each line is one JSON event
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for line in gz:
                    counts[json.loads(line)["type"]] += 1
    except (requests.RequestException, OSError, json.JSONDecodeError, KeyError) as e:
        logger.error(f"hour {hour} failed: {e}")
        raise  # re-raise so Prefect's retries kick in
    logger.info(f"hour {hour}: {counts.total()} events")
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    try:
        totals = sum((Counter(h) for h in hourly_counts), Counter())
    except TypeError as e:
        logger.error(f"combine failed: {e}")
        raise
    ordered = dict(totals.most_common())  # sorted by count, descending
    logger.info(f"combined {totals.total()} events across {len(hourly_counts)} hours")
    print(f"total events: {totals.total()}")
    return ordered


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
