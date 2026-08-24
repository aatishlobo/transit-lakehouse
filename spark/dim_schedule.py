"""SCD2 schedule dimension: `dim_stop_schedule`.

The crown jewel of the lakehouse, and the layer where the most silent-failure
modes cluster.

**The problem it solves.** Agencies republish GTFS-Static constantly. If the
schedule table is overwritten on each fetch, then every historical on-time
number is recomputed against a timetable that did not exist when the trip ran.
Nothing errors. Yesterday's OTP simply changes, and keeps changing, and the
metric becomes unauditable. Type 2 slowly-changing dimensions exist precisely
for this: rows are versioned with validity windows, never updated in place.

**Grain:** `(trip_id, stop_sequence)`, matching the fact grain minus
service_date. stop_id is an attribute -- invariant 3.2, a loop route serves the
same stop_id twice in one trip and keying on it collapses two scheduled events
into one.

**Change detection** is a hash over the schedule-relevant attributes only. A
new GTFS zip that differs in a shape file or a booking rule must NOT open a new
version of every stop time; otherwise the dimension grows by millions of rows
per fetch and the as-of join gets slower for no information gain.

**Validity is in observation time** (when we fetched the feed), not GTFS
`feed_info` dates. We can only honestly claim to know what the schedule was
from the moment we started fetching it. Overstating that would be inventing
history.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.session import RETENTION_PROPERTIES, build  # noqa: E402

DIM_PATH = "dim/dim_stop_schedule"
KEY = ["trip_id", "stop_sequence"]

# Sentinel for "still current". A NULL valid_to forces every as-of join to
# write `(valid_to IS NULL OR valid_to > x)`, which is easy to get wrong once
# and then wrong everywhere. A far-future timestamp keeps the predicate a
# simple BETWEEN.
FOREVER = "2999-01-01 00:00:00"

# Seed value for the FIRST version of a key. We began fetching GTFS-Static
# after the realtime archive had already been running, so no observed schedule
# version covers those earlier service dates. Two options existed: leave the
# initial valid_from at fetch time, which makes the as-of join match nothing
# for every historical trip, or seed it far in the past, which asserts the
# first schedule we saw was also in force before we saw it.
#
# The second is the standard SCD2 seeding choice and is almost always right in
# practice -- timetables change on the order of months. But it IS an
# assumption, not an observation, so every row carries `valid_from_assumed` and
# any analysis that cannot tolerate it can filter on that column. Stating the
# uncertainty in the data beats burying it in a README.
BEGINNING = "1970-01-01 00:00:00"

# Only these participate in change detection.
_ATTRS = [
    "stop_id",
    "arrival_time",
    "departure_time",
    "route_id",
    "service_id",
    "direction_id",
    "timepoint",
    "pickup_type",
    "drop_off_type",
]

_STOP_TIMES_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("stop_sequence", IntegerType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("pickup_type", StringType(), True),
        StructField("drop_off_type", StringType(), True),
        StructField("timepoint", StringType(), True),
    ]
)


def read_snapshot(spark: SparkSession, extracted_dir: str) -> DataFrame:
    """Read one extracted GTFS-Static snapshot into the dimension's shape."""
    d = Path(extracted_dir)
    # Read by NAME, not by position. Supplying an explicit schema alongside
    # header=True makes Spark map columns POSITIONALLY and ignore the header,
    # so a feed that adds a column anywhere on the left silently shifts every
    # field. GTFS-Static carries optional columns (location_group_id,
    # location_id, booking rules) whose presence varies by publisher, so
    # positional reads here are guaranteed to break eventually -- and to break
    # quietly, as all-NULL columns rather than an error.
    raw = spark.read.option("header", True).csv(str(d / "stop_times.txt"))
    missing = {f.name for f in _STOP_TIMES_SCHEMA} - set(raw.columns)
    if missing:
        raise RuntimeError(f"stop_times.txt is missing columns: {sorted(missing)}")
    st = raw.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in _STOP_TIMES_SCHEMA]
    ).filter(F.col("trip_id").isNotNull() & F.col("stop_sequence").isNotNull())
    trips = (
        spark.read.option("header", True)
        .csv(str(d / "trips.txt"))
        .select(
            "trip_id",
            "route_id",
            "service_id",
            F.col("direction_id").cast("int").alias("direction_id"),
        )
    )
    # Left join: a stop_time whose trip is missing from trips.txt is a broken
    # feed, and dropping it here would hide that. It surfaces as NULL route_id
    # and is counted by the orphan check below.
    return st.join(trips, on="trip_id", how="left")


def prepare(df: DataFrame, feed_sha: str, observed_at: str) -> DataFrame:
    """Add the change hash and SCD2 bookkeeping columns."""
    return (
        df.withColumn(
            "attr_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in _ATTRS]), 256),
        )
        .withColumn("feed_sha256", F.lit(feed_sha))
        .withColumn("valid_from", F.lit(observed_at).cast("timestamp"))
        .withColumn("valid_to", F.lit(FOREVER).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("valid_from_assumed", F.lit(False))
        .dropDuplicates(KEY)
    )


def upsert(spark: SparkSession, incoming: DataFrame, lake_root: str, observed_at: str) -> dict:
    """SCD2 upsert.

    Delta's MERGE cannot both close an old version and insert its replacement
    for the same key in one pass -- a source row matches at most one action.
    The standard resolution is to stage changed keys TWICE: once carrying the
    real key (which matches, and closes the old row) and once with a NULL
    merge key (which cannot match, and therefore inserts). That is what the
    union below does, and it is the only non-obvious part of this file.
    """
    target = f"{lake_root}/{DIM_PATH}"

    if not Path(target).exists():
        # Seeding load only. Later versions always carry observed timestamps.
        incoming = incoming.withColumn(
            "valid_from", F.lit(BEGINNING).cast("timestamp")
        ).withColumn("valid_from_assumed", F.lit(True))
        w = (
            incoming.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "false")
            .partitionBy("is_current")
        )
        for k, v in RETENTION_PROPERTIES.items():
            w = w.option(k, v)
        w.save(target)
        return {"created": True, "versions": incoming.count(), "closed": 0, "opened": 0}

    tbl = DeltaTable.forPath(spark, target)
    current = tbl.toDF().filter(F.col("is_current"))

    joined = incoming.alias("s").join(
        current.select(*KEY, F.col("attr_hash").alias("t_hash")).alias("t"),
        on=KEY,
        how="left",
    )
    changed = joined.filter(
        F.col("t_hash").isNotNull() & (F.col("s.attr_hash") != F.col("t_hash"))
    ).drop("t_hash")
    brand_new = joined.filter(F.col("t_hash").isNull()).drop("t_hash")

    n_changed, n_new = changed.count(), brand_new.count()

    # Rows that must INSERT carry a NULL merge key so they cannot match.
    inserts = changed.unionByName(brand_new).withColumn("_mergekey_trip", F.lit(None).cast("string"))
    # Rows that must CLOSE the existing version carry the real key.
    closes = changed.withColumn("_mergekey_trip", F.col("trip_id"))
    staged = closes.unionByName(inserts)

    (
        tbl.alias("t")
        .merge(
            staged.alias("s"),
            "t.trip_id = s._mergekey_trip AND t.stop_sequence = s.stop_sequence AND t.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                # Closed at the moment the new snapshot was observed, so the
                # windows tile with no gap and no overlap.
                "valid_to": F.lit(observed_at).cast("timestamp"),
                "is_current": F.lit(False),
            }
        )
        .whenNotMatchedInsert(
            values={c: F.col(f"s.{c}") for c in incoming.columns}
        )
        .execute()
    )
    return {"created": False, "closed": n_changed, "opened": n_changed + n_new}


def run(spark: SparkSession, extracted_dir: str, lake_root: str = "lake",
        observed_at: str | None = None) -> dict:
    d = Path(extracted_dir)
    feed_sha = d.name
    meta = d.parent.parent / f"fetched_dt=*/{feed_sha}.meta.json"
    if observed_at is None:
        hits = list(Path("data/static").glob(f"fetched_dt=*/{feed_sha}.meta.json"))
        observed_at = (
            json.loads(hits[0].read_text())["fetched_at"][:19].replace("T", " ")
            if hits
            else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        )

    snap = prepare(read_snapshot(spark, extracted_dir), feed_sha, observed_at)
    snap.cache()
    n = snap.count()
    orphans = snap.filter(F.col("route_id").isNull()).count()
    stats = upsert(spark, snap, lake_root, observed_at)
    snap.unpersist()

    stats.update({"snapshot_rows": n, "orphan_stop_times": orphans,
                  "observed_at": observed_at, "feed_sha": feed_sha})
    return stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--extracted", default=None, help="data/static/extracted/<sha>")
    ap.add_argument("--lake-root", default="lake")
    ap.add_argument("--observed-at", default=None, help="override, for testing SCD2")
    args = ap.parse_args()

    ex = args.extracted
    if ex is None:
        cands = sorted(Path("data/static/extracted").glob("*"))
        if not cands:
            raise SystemExit("no extracted GTFS-Static -- run ingest.static.fetch_static")
        ex = str(cands[-1])

    # stop_times is millions of rows; 8 shuffle partitions makes each task
    # large enough to OOM the local driver.
    spark = build("dim_schedule", shuffle_partitions=64, driver_memory="6g")
    print("DIM_SCHEDULE")
    stats = run(spark, ex, args.lake_root, args.observed_at)
    for k, v in stats.items():
        print(f"  {k}: {v}")
    spark.stop()


if __name__ == "__main__":
    main()
