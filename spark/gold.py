"""Gold: `fct_stop_arrival`, the append-only arrival fact table.

This is the table the whole project exists to produce, and the one MLE#1 reads.
It must satisfy the DE-to-ML contract in CLAUDE.md section 7 simultaneously
with being correct for historical analysis:

  1. append-only, with a TRUE event timestamp (never our poll clock)
  2. stable grain + idempotent identity, so Delta time travel can answer
     "what did this table look like as of prediction time T"
  3. provenance on every row, so label noise is filterable rather than hidden

Grain: (service_date, trip_id, stop_sequence). Not stop_id -- invariant 3.2,
loop routes serve the same stop_id twice in one trip and a stop_id grain
collapses two real events into one.

Resolver A only. Method C (settled TripUpdate predictions) is deliberately NOT
implemented here even though it would raise coverage, because MLE#1 uses
TripUpdate predictions as features: labels derived from C would let the model
learn to copy the agency's forecast and post an excellent, meaningless MAE.
That is the leakage trap in CLAUDE.md 7.3. Labels come from positions,
features come from predictions, and the two never mix.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.session import RETENTION_PROPERTIES, build  # noqa: E402

RESOLVER_VERSION = "2.0.0-spark"

GRAIN = ["service_date", "trip_id", "stop_sequence"]


def resolve_arrivals(spark: SparkSession, lake_root: str = "lake") -> DataFrame:
    """Derive arrivals from vehicle positions. Resolver A.

    The signal is a state transition: the agency reports current_status ==
    STOPPED_AT together with a stop. That is the agency asserting the vehicle
    is physically at that stop.
    """
    src = f"{lake_root}/silver/vehicle_positions"
    if not Path(src).exists():
        raise FileNotFoundError(f"no silver table at {src} -- run spark.silver first")

    vp = spark.read.format("delta").load(src)

    stopped = vp.filter(
        (F.col("current_status") == "STOPPED_AT")
        & F.col("current_stop_id").isNotNull()
        & F.col("grain_complete")
        & (~F.col("is_deleted_entity"))
        & F.col("vehicle_report_ts").isNotNull()
    ).withColumnRenamed("current_stop_sequence", "stop_sequence")

    # THE arrival is the FIRST sighting, not the last. A vehicle waiting at a
    # stop reports STOPPED_AT on every poll for the whole dwell; the last such
    # report measures departure, and an intermediate one measures nothing.
    # Ordering by the vehicle's OWN timestamp, never our ingest time.
    w = Window.partitionBy(*[F.col(c) for c in GRAIN]).orderBy(
        F.col("vehicle_report_ts").asc()
    )
    w_all = Window.partitionBy(*[F.col(c) for c in GRAIN])

    arrivals = (
        stopped.withColumn("_rn", F.row_number().over(w))
        # How many consecutive polls saw this vehicle stopped here. This is the
        # dwell-observation count, and it is the raw material for the selection
        # -bias measurement: capture probability scales with dwell time, so
        # long-dwell stops are over-represented in this very table.
        .withColumn("n_stopped_observations", F.count(F.lit(1)).over(w_all))
        .withColumn("last_stopped_ts", F.max("vehicle_report_ts").over(w_all))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return arrivals.select(
        # --- grain -------------------------------------------------------
        F.col("service_date"),
        F.col("trip_id"),
        F.col("stop_sequence"),
        # --- attributes (stop_id is an ATTRIBUTE, never identity) --------
        F.col("current_stop_id").alias("stop_id"),
        F.col("route_id"),
        F.col("direction_id"),
        F.col("vehicle_id"),
        # --- the fact ----------------------------------------------------
        # Event time, from the producer. Invariant 3.4: training on ingest_ts
        # leaks, because ingest_ts encodes when we happened to look.
        F.col("vehicle_report_ts").alias("actual_arrival_ts"),
        F.col("last_stopped_ts").alias("last_stopped_ts"),
        (
            F.unix_timestamp("last_stopped_ts") - F.unix_timestamp("vehicle_report_ts")
        ).alias("observed_dwell_s"),
        # --- provenance (CLAUDE.md section 2) ----------------------------
        F.lit("stopped_at").alias("arrival_method"),
        F.lit("high").alias("arrival_confidence"),
        F.col("n_stopped_observations"),
        # The sampling granularity at that moment IS the error bar on this
        # timestamp. Stored per row so a future cadence change cannot silently
        # invalidate older rows.
        F.col("poll_interval_s"),
        F.lit(None).cast("double").alias("arrival_method_agreement_s"),
        F.lit(RESOLVER_VERSION).alias("resolver_version"),
        F.col("decoder_version"),
        # --- lineage -----------------------------------------------------
        F.col("ingest_ts"),
        F.col("feed_header_ts"),
        F.col("source_path"),
    )


def merge(spark: SparkSession, arrivals: DataFrame, lake_root: str = "lake") -> dict:
    """Idempotent MERGE on the gold grain. Invariant 3.7.

    Deliberately at-least-once + idempotent MERGE rather than exactly-once.
    Exactly-once across a read-process-write cycle buys complexity we do not
    need, because the grain already makes duplicates harmless: replays and
    duplicate deliveries converge to the same table.

    The matched condition is not a blind overwrite. An existing row is
    replaced ONLY by an EARLIER sighting, because the arrival is the first
    sighting -- so late-arriving data can correct a row, but reprocessing can
    never push a timestamp later. That single condition is what makes this
    both idempotent and correct under out-of-order input.
    """
    target = f"{lake_root}/gold/fct_stop_arrival"

    if not Path(target).exists():
        writer = (
            arrivals.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "false")
            .partitionBy("service_date")
        )
        for k, v in RETENTION_PROPERTIES.items():
            writer = writer.option(k, v)
        writer.save(target)
        return {"created": True, "rows": arrivals.count()}

    tbl = DeltaTable.forPath(spark, target)
    cond = " AND ".join(f"t.{c} <=> s.{c}" for c in GRAIN)

    before = spark.read.format("delta").load(target).count()
    (
        tbl.alias("t")
        .merge(arrivals.alias("s"), cond)
        .whenMatchedUpdateAll(condition="s.actual_arrival_ts < t.actual_arrival_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )
    after = spark.read.format("delta").load(target).count()
    return {"created": False, "rows_before": before, "rows_after": after,
            "inserted": after - before}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default="lake")
    args = ap.parse_args()

    spark = build("gold")
    print("GOLD")
    arrivals = resolve_arrivals(spark, args.lake_root)
    arrivals.cache()
    n = arrivals.count()
    n_keys = arrivals.select(*GRAIN).distinct().count()
    print(f"  resolved {n:,} arrivals, {n_keys:,} distinct grain keys")
    if n != n_keys:
        raise SystemExit(f"GRAIN VIOLATION: {n:,} rows but {n_keys:,} keys")

    stats = merge(spark, arrivals, args.lake_root)
    print(f"  merge: {stats}")
    arrivals.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
