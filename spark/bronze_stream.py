"""Bronze as a Spark **Structured Streaming** job.

Same contract as `spark.bronze` (decode, explode, write Delta partitioned by
service_date) but driven by a streaming read over the archive directory rather
than a batch listing. Two things this buys that the batch version hand-rolled:

**1. The checkpoint replaces the processed-files table.** The batch job tracked
consumed files in `lake/_bronze_processed_files` so a re-run would not
re-append. Structured Streaming's file source does exactly that, durably, in
the checkpoint -- and it does it as part of the same commit as the write, so
the two cannot diverge after a crash between them.

**2. New polls are picked up without a scheduler.** The poller writes a file
every 120s; the stream notices it. `availableNow` gives batch-like semantics
for a one-shot catch-up run, and the same code runs continuously with a
processing-time trigger.

**Why bronze stays dumb.** A streaming checkpoint is coupled to the query plan:
change the shape of this query and the checkpoint cannot be resumed, which
means reprocessing everything. So bronze decodes and writes and does nothing
else -- every piece of logic that might still change lives in silver and gold,
where a full recompute is cheap. That constraint is the reason this file was
easy to add at all.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from spark.bronze import _decode_partition  # reuse the identical decode path
from spark.schemas import SCHEMAS  # noqa: E402
from spark.session import RETENTION_PROPERTIES, build  # noqa: E402


# The binaryFile source's own schema. A streaming read cannot infer one --
# inference would require listing the directory, which is exactly the
# unbounded operation streaming exists to avoid -- so it must be declared.
BINARY_FILE_SCHEMA = StructType(
    [
        StructField("path", StringType(), False),
        StructField("modificationTime", TimestampType(), False),
        StructField("length", LongType(), False),
        StructField("content", BinaryType(), True),
    ]
)


def _decoded_schema(feed_type: str) -> StructType:
    return StructType(
        [
            StructField("source_path", StringType(), False),
            StructField("poll_epoch", LongType(), True),
            StructField("decode_error", StringType(), True),
            StructField("payload", SCHEMAS[feed_type], True),
        ]
    )


def _write_batch(batch_df: DataFrame, batch_id: int, feed_type: str,
                 lake_root: str, poll_interval_s: int) -> None:
    """foreachBatch sink.

    The decode is a Python-level operation over raw bytes, so it runs through
    mapPartitions on the micro-batch rather than as a SQL expression. Using the
    SAME `_decode_partition` as the batch job is deliberate: two decode paths
    would be two places for the absent-vs-zero invariant to drift.
    """
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession
    decoded = spark.createDataFrame(
        batch_df.select("path", "content").rdd.mapPartitions(
            lambda rows: _decode_partition(rows, feed_type, poll_interval_s)
        ),
        _decoded_schema(feed_type),
    )
    decoded.persist()

    failures = decoded.filter(F.col("decode_error").isNotNull())
    if not failures.rdd.isEmpty():
        # Invariant 3.9: quarantine, never discard. The bytes are still in the
        # archive; this makes the failure countable.
        (
            failures.select("source_path", "poll_epoch", "decode_error")
            .write.format("delta").mode("append")
            .save(f"{lake_root}/bronze/_decode_failures")
        )

    rows = (
        decoded.filter(F.col("decode_error").isNull())
        .select("source_path", F.col("payload.*"))
        # Invariant 3.4: ingest_ts is the POLL time, taken from the archive
        # filename. decode.py stamps datetime.now(), which on a replay would
        # brand old polls with today's clock.
        .withColumn("ingest_ts", F.to_timestamp(F.from_unixtime(F.col("ingest_ts").cast("long"))))
        .withColumn("bronze_loaded_at", F.current_timestamp())
    )

    (
        rows.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")  # schema evolution OFF on write paths
        .partitionBy("service_date")     # invariant 3.3: service date is a ROW property
        .save(f"{lake_root}/bronze/{feed_type}")
    )
    decoded.unpersist()


def run(
    spark: SparkSession,
    feed_type: str,
    data_root: str = "data",
    lake_root: str = "lake",
    poll_interval_s: int = 120,
    once: bool = True,
    max_files_per_trigger: int = 200,
) -> dict:
    src = f"{data_root}/raw/{feed_type}"
    if not Path(src).exists():
        raise FileNotFoundError(f"no archive at {src}")

    target = f"{lake_root}/bronze/{feed_type}"
    checkpoint = f"{lake_root}/_checkpoints/bronze_{feed_type}"

    if not Path(target).exists():
        # Create the table first so retention is pinned from version 0 rather
        # than applied later (invariant 3.8 -- a default VACUUM would destroy
        # the time-travel window the ML contract depends on).
        # Bootstrap with EXACTLY the shape _write_batch produces. Deriving it
        # any other way invites a mismatch that only appears on the first real
        # micro-batch, as DELTA_FAILED_TO_MERGE_FIELDS -- schema evolution is
        # off on write paths precisely so this fails loudly instead of
        # silently widening the column.
        empty = (
            spark.createDataFrame([], SCHEMAS[feed_type])
            .withColumn("source_path", F.lit(None).cast("string"))
            .withColumn(
                "ingest_ts",
                F.to_timestamp(F.from_unixtime(F.col("ingest_ts").cast("long"))),
            )
            .withColumn("bronze_loaded_at", F.current_timestamp())
        )
        # Column ORDER must match too, not just names and types.
        cols = ["source_path"] + [
            c for c in SCHEMAS[feed_type].fieldNames()
        ] + ["bronze_loaded_at"]
        w = (
            empty.select(*cols)
            .write.format("delta").mode("overwrite").partitionBy("service_date")
        )
        for k, v in RETENTION_PROPERTIES.items():
            w = w.option(k, v)
        w.save(target)

    stream = (
        spark.readStream.format("binaryFile")
        .schema(BINARY_FILE_SCHEMA)
        .option("pathGlobFilter", "*.pb.gz")
        .option("recursiveFileLookup", "true")
        # Bounds micro-batch size so a 19-day cold start does not try to decode
        # the whole archive in one batch and OOM the driver.
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(src)
    )

    writer = (
        stream.writeStream
        # The checkpoint is what makes this exactly-once over FILES: the source
        # offset (which files have been consumed) commits atomically with the
        # sink, so a crash between decode and write cannot lose or duplicate a
        # poll. This is what the batch job's processed-files table approximated.
        .option("checkpointLocation", checkpoint)
        .foreachBatch(
            lambda df, bid: _write_batch(df, bid, feed_type, lake_root, poll_interval_s)
        )
    )
    # availableNow: consume everything currently on disk, then stop -- batch
    # semantics with streaming bookkeeping. processingTime: stay up and pick up
    # each new poll as the poller writes it.
    trigger = {"availableNow": True} if once else {"processingTime": "120 seconds"}
    q = writer.trigger(**trigger).start()

    q.awaitTermination()
    prog = q.lastProgress or {}
    return {
        "feed": feed_type,
        "batches": q.recentProgress and len(q.recentProgress) or 0,
        "rows_last_batch": prog.get("numInputRows"),
        "checkpoint": checkpoint,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", choices=["trip_updates", "vehicle_positions", "both"],
                    default="vehicle_positions")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--lake-root", default="lake")
    ap.add_argument("--poll-interval-s", type=int, default=120)
    ap.add_argument("--max-files-per-trigger", type=int, default=200)
    ap.add_argument("--continuous", action="store_true",
                    help="run forever on a 120s trigger instead of availableNow")
    args = ap.parse_args()

    spark = build("bronze-stream", shuffle_partitions=16, driver_memory="6g")
    print("BRONZE (structured streaming)")
    feeds = ["trip_updates", "vehicle_positions"] if args.feed == "both" else [args.feed]
    for feed in feeds:
        stats = run(
            spark, feed,
            data_root=args.data_root, lake_root=args.lake_root,
            poll_interval_s=args.poll_interval_s,
            once=not args.continuous,
            max_files_per_trigger=args.max_files_per_trigger,
        )
        print(f"  {stats}")
    spark.stop()


if __name__ == "__main__":
    main()
