"""`fct_stop_otp`: derived arrivals joined as-of to the schedule in force.

This is the table that answers the actual question -- **was the vehicle on
time**, where "on time" means against the published timetable rather than
against the agency's own live prediction.

Three things have to be right, and all three fail silently:

**1. As-of, not current.** The dimension row selected is the one whose validity
window contains the trip's service day, not the row that happens to be current
today. Joining to `is_current` recomputes history every time an agency
republishes, so last month's OTP changes without anyone touching last month's
data.

**2. Join conservation.** A LEFT join, with the fact row count asserted
identical before and after. An inner join here would silently delete every
arrival whose trip is absent from the schedule -- and those absences are not
random, they concentrate in added/unscheduled service, which is exactly the
service most likely to be late. The metric would improve because the bad rows
disappeared.

**3. Scheduled-time arithmetic.** Delegated wholly to `spark.scheduled_time`,
which implements the noon-minus-12h rule so both DST transitions land right.
See invariant 3.6.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.scheduled_time import service_day_start_epochs  # noqa: E402
from spark.session import RETENTION_PROPERTIES, build  # noqa: E402

OTP_PATH = "gold/fct_stop_otp"

# The industry convention: "on time" is a window, not an instant, and it is
# asymmetric because arriving early is a different failure from arriving late
# (an early bus is one you miss entirely).
ON_TIME_EARLY_S = -60
ON_TIME_LATE_S = 300


def build_otp(spark: SparkSession, lake_root: str = "lake") -> tuple[DataFrame, dict]:
    facts = spark.read.format("delta").load(f"{lake_root}/gold/fct_stop_arrival")
    dim = spark.read.format("delta").load(f"{lake_root}/dim/dim_stop_schedule")

    n_facts = facts.count()

    # Service-day starts computed in Python (zoneinfo gets DST exactly right)
    # and broadcast as integers. One entry per service_date, not per row.
    dates = [r["service_date"] for r in facts.select("service_date").distinct().collect()]
    lookup = service_day_start_epochs(dates)
    if not lookup:
        raise RuntimeError("no valid service_dates in the fact table")

    start_map = F.create_map(*[x for kv in lookup.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    facts = facts.withColumn("service_day_start_epoch", start_map[F.col("service_date")])

    # The as-of instant: the start of the service day the trip belongs to. Not
    # the arrival timestamp -- a trip that runs past midnight must resolve
    # against the schedule for ITS service day, not the next one's.
    facts = facts.withColumn(
        "as_of_ts", F.to_timestamp(F.from_unixtime(F.col("service_day_start_epoch")))
    )

    d = dim.select(
        F.col("trip_id").alias("d_trip_id"),
        F.col("stop_sequence").alias("d_stop_sequence"),
        F.col("arrival_time").alias("scheduled_arrival_time"),
        F.col("departure_time").alias("scheduled_departure_time"),
        F.col("stop_id").alias("scheduled_stop_id"),
        F.col("route_id").alias("scheduled_route_id"),
        F.col("service_id"),
        F.col("timepoint"),
        F.col("valid_from"),
        F.col("valid_to"),
        F.col("feed_sha256"),
    )

    joined = facts.join(
        d,
        (F.col("trip_id") == F.col("d_trip_id"))
        & (F.col("stop_sequence") == F.col("d_stop_sequence"))
        # The as-of predicate. Half-open interval so tiling windows can never
        # match twice for a single instant.
        & (F.col("valid_from") <= F.col("as_of_ts"))
        & (F.col("as_of_ts") < F.col("valid_to")),
        how="left",
    )

    # GTFS times parsed with the same regex the unit tests pin, supporting
    # hours >= 24 (25:14:00 is 1:14am the next day and is valid).
    secs = (
        F.regexp_extract("scheduled_arrival_time", r"^(\d{1,3}):", 1).cast("long") * 3600
        + F.regexp_extract("scheduled_arrival_time", r"^\d{1,3}:(\d{2}):", 1).cast("long") * 60
        + F.regexp_extract("scheduled_arrival_time", r":(\d{2})$", 1).cast("long")
    )
    valid_time = F.col("scheduled_arrival_time").rlike(r"^\d{1,3}:[0-5]\d:[0-5]\d$")

    out = (
        joined.withColumn(
            "scheduled_arrival_ts",
            F.when(
                valid_time,
                F.to_timestamp(F.from_unixtime(F.col("service_day_start_epoch") + secs)),
            ).otherwise(F.lit(None).cast("timestamp")),
        )
        .withColumn(
            "delay_s",
            F.when(
                F.col("scheduled_arrival_ts").isNotNull(),
                F.unix_timestamp("actual_arrival_ts") - F.unix_timestamp("scheduled_arrival_ts"),
            ),
        )
        .withColumn(
            "is_on_time",
            F.when(
                F.col("delay_s").isNotNull(),
                F.col("delay_s").between(ON_TIME_EARLY_S, ON_TIME_LATE_S),
            ),
        )
        .withColumn("schedule_matched", F.col("scheduled_arrival_time").isNotNull())
        .drop("d_trip_id", "d_stop_sequence")
    )

    n_out = out.count()
    matched = out.filter("schedule_matched").count()
    scored = out.filter(F.col("delay_s").isNotNull()).count()

    stats = {
        "facts_in": n_facts,
        "rows_out": n_out,
        "join_conserved": n_facts == n_out,
        "schedule_matched": matched,
        "match_rate_pct": round(100 * matched / n_facts, 1) if n_facts else 0.0,
        "scored": scored,
    }
    return out, stats


def run(spark: SparkSession, lake_root: str = "lake") -> dict:
    out, stats = build_otp(spark, lake_root)

    # Join conservation is an assertion, not a metric. A LEFT join that changed
    # the row count means a duplicate dimension version matched -- exactly the
    # thing the "one current version per key" SCD2 test guards.
    if not stats["join_conserved"]:
        raise SystemExit(
            f"JOIN CONSERVATION FAILED: {stats['facts_in']} facts -> {stats['rows_out']} rows"
        )

    target = f"{lake_root}/{OTP_PATH}"
    w = (
        out.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .option("overwriteSchema", "true")
        .partitionBy("service_date")
    )
    if not Path(target).exists():
        for k, v in RETENTION_PROPERTIES.items():
            w = w.option(k, v)
    w.save(target)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default="lake")
    args = ap.parse_args()

    spark = build("fct_otp", shuffle_partitions=32, driver_memory="6g")
    print("FCT_OTP")
    for k, v in run(spark, args.lake_root).items():
        print(f"  {k}: {v}")
    spark.stop()


if __name__ == "__main__":
    main()
