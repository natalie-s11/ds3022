#!/usr/bin/env python3

"""
Seismic Session Windows by Flynn Region

Reads the "seismic-backfill" topic (from seismic-backfill.py) and clusters
earthquakes into SESSIONS per Flynn region: a session stays open while quakes
keep arriving in the same region, and closes once that region has been quiet
for INACTIVITY_GAP (in quake ORIGIN time, not Kafka time).

Pipeline:
  1. Flatten each GeoJSON feature to {unid, time, mag, region, depth}
  2. Drop quakes of magnitude <= MIN_MAG (3.9)
  3. Re-key by flynn_region (group_by repartitions the stream by region)
  4. Session window per region, collect() every quake into a list
  5. Drop "quiet" sessions with fewer than MIN_EVENTS quakes

Note: a session only closes when a LATER quake (in any region on the same
partition) moves event time past its end + grace. With a one-shot backfill,
the most recent sessions stay open until new data arrives.

Re-running: state and offsets are kept under CONSUMER_GROUP. To replay from
the start, change CONSUMER_GROUP or set RESET=1 (clears local state too).
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "seismic-backfill"
CONSUMER_GROUP = "seismic-region-sessions"
MIN_MAG = 3.9                         # keep only quakes with mag > MIN_MAG
MIN_EVENTS = 3                        # sessions with fewer quakes are "not meaningful"
INACTIVITY_GAP = timedelta(hours=6)   # region quiet this long -> session closes
GRACE = timedelta(minutes=30)         # tolerance for out-of-order origin times

# Log to a local file and the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("seismic-region-sessions.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# Use the quake's ORIGIN time (ms) as the event timestamp, not the produce time
def origin_ts(value, headers, timestamp, timestamp_type) -> int:
    try:
        t = value["data"]["properties"]["time"]
        return int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp() * 1000)
    except (KeyError, TypeError, ValueError):
        return timestamp  # malformed: fall back to Kafka time; flatten() drops it anyway


# Pull the fields we need out of the GeoJSON feature; None if malformed
def flatten(value):
    try:
        p = value["data"]["properties"]
        return {"unid": p["unid"], "time": p["time"], "mag": p.get("mag"),
                "depth": p.get("depth"), "region": p["flynn_region"]}
    except (KeyError, TypeError) as e:
        log.warning(f"skipping malformed message: {e}")
        return None


# Turn a closed session {start, end, value: [quakes]} into a summary row
def summarize(session):
    quakes = session["value"]
    ms_to_iso = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
    mags = [q["mag"] for q in quakes]
    return {
        "region": quakes[0]["region"],
        "start": ms_to_iso(session["start"]),
        "end": ms_to_iso(session["end"]),
        "count": len(quakes),
        "max_mag": max(mags),
        "avg_mag": round(sum(mags) / len(mags), 2),
        "quakes": quakes,  # the aggregated list of every quake in the session
    }


# Log one meaningful session as a header line plus one line per quake
def report(s):
    log.info(f"SESSION {s['region']} | {s['start']} -> {s['end']} UTC | "
             f"n={s['count']} max=M{s['max_mag']} avg=M{s['avg_mag']}")
    for q in s["quakes"]:
        log.info(f"    {q['time']} M{q['mag']} depth={q['depth']}km")


def main():
    app = Application(
        broker_address=KAFKA_BROKER,
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        consumer_extra_config={"broker.address.family": "v4"},
        producer_extra_config={"broker.address.family": "v4"},
    )
    if os.getenv("RESET") == "1":
        app.clear_state()

    topic = app.topic(TOPIC, value_deserializer="json", timestamp_extractor=origin_ts)
    sdf = app.dataframe(topic)

    # 1-2. Flatten, drop malformed and small quakes (mag <= 3.9)
    sdf = sdf.apply(flatten).filter(lambda q: q is not None and q["mag"] is not None and q["mag"] > MIN_MAG)

    # 3. Re-key by region so each region gets its own sessions
    sdf = sdf.group_by(lambda q: q["region"], name="flynn_region")

    # 4. Session window per region, aggregating quakes into a list; emit only closed sessions
    sdf = sdf.session_window(inactivity_gap_ms=INACTIVITY_GAP, grace_ms=GRACE).collect().final()

    # 5. Keep only active regions, summarize, and report
    sdf = sdf.filter(lambda s: len(s["value"]) >= MIN_EVENTS).apply(summarize).update(report)

    log.info(f"Sessionizing '{TOPIC}' by Flynn region: mag>{MIN_MAG}, gap={INACTIVITY_GAP}, min_events={MIN_EVENTS}")
    try:
        app.run()
    except KeyboardInterrupt:
        log.info("Stopped")
    except Exception as e:
        log.exception(f"Session windowing failed: {e}")


if __name__ == "__main__":
    main()
