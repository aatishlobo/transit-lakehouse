"""Explicit Spark schemas for the decoded GTFS-Realtime feeds.

Written out rather than inferred, deliberately. Schema inference on a UDF
result is a guess made from whatever rows happen to arrive first, and it
silently changes when the feed changes. These schemas are the Spark-side
statement of the same contract `streaming/contracts.py` enforces on Kafka.

EVERY numeric field is nullable. That is not laziness: invariant 3.1 says
absent is not zero, and a non-nullable Int would force a default. If a column
here ever becomes non-nullable, that is a bug.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Envelope stamped on every row by decode.FeedMeta.
_META_FIELDS = [
    StructField("feed_type", StringType(), True),
    StructField("feed_header_ts", StringType(), True),
    StructField("feed_header_ts_epoch", LongType(), True),
    StructField("gtfs_realtime_version", StringType(), True),
    StructField("incrementality", StringType(), True),
    # NOTE: decode.py stamps this at decode time. Bronze overwrites it with
    # the poll epoch taken from the archive filename -- see bronze.py.
    StructField("ingest_ts", StringType(), True),
    StructField("payload_sha256", StringType(), True),
    StructField("poll_interval_s", IntegerType(), True),
    StructField("decoder_version", StringType(), True),
    StructField("protobuf_runtime", StringType(), True),
]

# TripDescriptor, flattened. service_date is GTFS start_date (YYYYMMDD) and is
# authoritative -- invariant 3.3 forbids deriving it from any timestamp.
_TRIP_FIELDS = [
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("direction_id", IntegerType(), True),
    StructField("trip_start_time", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("trip_schedule_relationship", StringType(), True),
]

_ENTITY_FIELDS = [
    StructField("entity_id", StringType(), True),
    StructField("is_deleted", BooleanType(), True),
]

_VEHICLE_ID_FIELDS = [
    StructField("vehicle_id", StringType(), True),
    StructField("vehicle_label", StringType(), True),
]

TRIP_UPDATES_SCHEMA = StructType(
    _META_FIELDS
    + _ENTITY_FIELDS
    + _TRIP_FIELDS
    + _VEHICLE_ID_FIELDS
    + [
        StructField("trip_update_ts", StringType(), True),
        StructField("trip_update_ts_epoch", LongType(), True),
        StructField("trip_delay_s", IntegerType(), True),
        StructField("n_stop_time_updates", IntegerType(), True),
        # stop_sequence is identity; stop_id is an attribute. Invariant 3.2:
        # loop routes serve the same stop_id twice in one trip.
        StructField("stop_sequence", IntegerType(), True),
        StructField("stop_id", StringType(), True),
        StructField("stop_schedule_relationship", StringType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("arrival_time_epoch", LongType(), True),
        StructField("arrival_delay_s", IntegerType(), True),
        StructField("arrival_uncertainty", IntegerType(), True),
        StructField("departure_time", StringType(), True),
        StructField("departure_time_epoch", LongType(), True),
        StructField("departure_delay_s", IntegerType(), True),
        StructField("departure_uncertainty", IntegerType(), True),
    ]
)

VEHICLE_POSITIONS_SCHEMA = StructType(
    _META_FIELDS
    + _ENTITY_FIELDS
    + _TRIP_FIELDS
    + _VEHICLE_ID_FIELDS
    + [
        StructField("vehicle_report_ts", StringType(), True),
        StructField("vehicle_report_ts_epoch", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("bearing", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("current_stop_sequence", IntegerType(), True),
        StructField("current_stop_id", StringType(), True),
        # Resolver A's entire signal. STOPPED_AT + a stop_id is the agency
        # saying "this vehicle is at this stop right now".
        StructField("current_status", StringType(), True),
        StructField("congestion_level", StringType(), True),
        StructField("occupancy_status", StringType(), True),
        StructField("occupancy_percentage", IntegerType(), True),
    ]
)

SCHEMAS = {
    "trip_updates": TRIP_UPDATES_SCHEMA,
    "vehicle_positions": VEHICLE_POSITIONS_SCHEMA,
}

ARRAY_SCHEMAS = {k: ArrayType(v) for k, v in SCHEMAS.items()}
