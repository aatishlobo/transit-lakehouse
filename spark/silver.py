"""Silver: typed, deduplicated, quality-flagged.

All volatile logic lives here and in gold, never in bronze (CLAUDE.md section
6): a streaming checkpoint couples to the query plan, so a bronze job that
changes shape cannot resume from its own checkpoint. Bronze stays dumb so it
can stay running.

Silver does three things and no more:
  1. casts the decoder's ISO strings to real timestamps, in UTC
  2. deduplicates on producer-side identity
  3. flags rows that cannot participate in the grain

It does NOT drop rows. Dropping here would make coverage unmeasurable, and
per-operator coverage is itself one of the project's findings.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.session import RETENTION_PROPERTIES, build  # noqa: E402

# Producer-side identity. Deliberately excludes our poll time: a restarted
# poller re-fetches and re-produces a byte-identical payload, and keying on
# when WE saw it would turn that no-op into a duplicate row.
DEDUP_KEYS = {
    "trip_updates": ["feed_header_ts_epoch", "entity_id", "stop_sequence"],
    "vehicle_positions": ["feed_header_ts_epoch", "entity_id"],
}

_TS_COLUMNS = {
    "trip_updates": ["feed_header_ts", "trip_update_ts", "arrival_time", "departure_time"],
    "vehicle_positions": ["feed_header_ts", "vehicle_report_ts"],
}


def _to_utc_timestamp(df: DataFrame, cols: list[str]) -> DataFrame:
    """Cast ISO-8601 strings to timestamps.

    Session timeZone is UTC (invariant 3.5), and the decoder emits offset-aware
    ISO strings, so this is a parse rather than a conversion. Converting to
    America/Los_Angeles happens exactly once, in the serving layer -- doing it
    here would shift every hour-of-day aggregate by 7-8 hours.
    """
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df


def run(
    spark: SparkSession,
    feed_type: str,
    lake_root: str = "lake",
) -> DataFrame:
    src = f"{lake_root}/bronze/{feed_type}"
    if not Path(src).exists():
        raise FileNotFoundError(f"no bronze table at {src} -- run spark.bronze first")

    df = spark.read.format("delta").load(src)
    n_in = df.count()

    df = _to_utc_timestamp(df, _TS_COLUMNS[feed_type])

    # Deterministic dedup: keep the earliest-received copy of each producer
    # event. Deterministic matters -- a nondeterministic tiebreak makes the
    # whole pipeline non-reproducible and quietly breaks the idempotence test.
    keys = [k for k in DEDUP_KEYS[feed_type] if k in df.columns]
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(
        F.col("ingest_ts").asc_nulls_last(), F.col("source_path").asc()
    )
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # Quality flags, not filters. A row that cannot join the grain is still
    # evidence about the operator that produced it.
    grain_cols = ["service_date", "trip_id"]
    seq_col = "stop_sequence" if feed_type == "trip_updates" else "current_stop_sequence"
    df = df.withColumn(
        "grain_complete",
        F.col("service_date").isNotNull()
        & F.col("trip_id").isNotNull()
        & F.col(seq_col).isNotNull(),
    ).withColumn(
        "is_deleted_entity", F.coalesce(F.col("is_deleted"), F.lit(False))
    )

    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    target = f"{lake_root}/silver/{feed_type}"
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .option("overwriteSchema", "true")
        .partitionBy("service_date")
    )
    if not Path(target).exists():
        for k, v in RETENTION_PROPERTIES.items():
            writer = writer.option(k, v)
    writer.save(target)

    n_out = df.count()
    n_grain = df.filter(F.col("grain_complete")).count()
    print(
        f"  {feed_type}: {n_in:,} -> {n_out:,} rows "
        f"({n_in - n_out:,} dups removed), {n_grain:,} grain-complete"
    )
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", choices=["trip_updates", "vehicle_positions", "both"], default="both")
    ap.add_argument("--lake-root", default="lake")
    args = ap.parse_args()

    spark = build("silver")
    feeds = ["trip_updates", "vehicle_positions"] if args.feed == "both" else [args.feed]
    print("SILVER")
    for feed in feeds:
        run(spark, feed, lake_root=args.lake_root)
    spark.stop()


if __name__ == "__main__":
    main()
