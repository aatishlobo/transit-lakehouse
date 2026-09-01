"""Land the extracted GTFS-Static CSVs as a plain Delta staging table.

Deliberately does NO versioning. Its only job is to turn CSV into a typed Delta
table representing "the schedule as of the latest fetch", so that dbt can own
the Type-2 history via `dbt snapshot`.

Splitting it this way puts the SCD2 logic in the tool whose snapshot mechanism
exists for exactly that, and keeps the messy part (CSV parsing, GTFS's optional
columns) in Spark where it belongs.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.session import RETENTION_PROPERTIES, build  # noqa: E402

STAGING_PATH = "staging/gtfs_stop_schedule"

_COLS = StructType(
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


def run(spark: SparkSession, extracted_dir: str, lake_root: str = "lake",
        observed_at: str | None = None) -> dict:
    d = Path(extracted_dir)
    feed_sha = d.name

    if observed_at is None:
        hits = list(Path("data/static").glob(f"fetched_dt=*/{feed_sha}.meta.json"))
        observed_at = (
            json.loads(hits[0].read_text())["fetched_at"][:19].replace("T", " ")
            if hits else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        )

    # Select by NAME. header=True plus an explicit schema makes Spark map
    # columns POSITIONALLY and ignore the header, and GTFS's optional columns
    # (location_group_id, booking rules) vary by publisher, so a positional
    # read silently shifts every field.
    raw = spark.read.option("header", True).csv(str(d / "stop_times.txt"))
    missing = {f.name for f in _COLS} - set(raw.columns)
    if missing:
        raise RuntimeError(f"stop_times.txt missing columns: {sorted(missing)}")

    st = raw.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in _COLS]
    ).filter(F.col("trip_id").isNotNull() & F.col("stop_sequence").isNotNull())

    trips = (
        spark.read.option("header", True).csv(str(d / "trips.txt"))
        .select("trip_id", "route_id", "service_id",
                F.col("direction_id").cast("int").alias("direction_id"))
    )

    out = (
        st.join(trips, on="trip_id", how="left")
        # Composite grain flattened into one key: dbt snapshots take a single
        # unique_key, and (trip_id, stop_sequence) is the grain -- stop_id is
        # an attribute, since a loop route serves it twice in one trip.
        .withColumn("schedule_key", F.concat_ws(":", "trip_id", "stop_sequence"))
        .withColumn("feed_sha256", F.lit(feed_sha))
        .withColumn("observed_at", F.lit(observed_at).cast("timestamp"))
        .dropDuplicates(["schedule_key"])
    )

    target = f"{lake_root}/{STAGING_PATH}"
    w = (
        out.write.format("delta").mode("overwrite")
        .option("mergeSchema", "false").option("overwriteSchema", "true")
    )
    if not Path(target).exists():
        for k, v in RETENTION_PROPERTIES.items():
            w = w.option(k, v)
    w.save(target)

    n = out.count()
    return {"rows": n, "feed_sha": feed_sha, "observed_at": observed_at, "path": target}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--extracted", default=None)
    ap.add_argument("--lake-root", default="lake")
    ap.add_argument("--observed-at", default=None)
    args = ap.parse_args()

    ex = args.extracted
    if ex is None:
        cands = sorted(Path("data/static/extracted").glob("*"))
        if not cands:
            raise SystemExit("no extracted GTFS-Static -- run `make static`")
        ex = str(cands[-1])

    spark = build("stage-gtfs", shuffle_partitions=64, driver_memory="6g")
    print("STAGE_GTFS")
    for k, v in run(spark, ex, args.lake_root, args.observed_at).items():
        print(f"  {k}: {v}")
    spark.stop()


if __name__ == "__main__":
    main()
