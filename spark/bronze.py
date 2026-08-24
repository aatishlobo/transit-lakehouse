"""Bronze: raw archived protobuf -> Delta, one row per exploded entity.

Design rule (CLAUDE.md section 6): **bronze is dumb and stable.** Streaming
checkpoints couple to the query plan, so every piece of logic that might later
change belongs in silver or gold. Bronze decodes, stamps lineage, and writes.
It makes no judgements about the data.

The decode itself reuses `ingest.poller.decode` unmodified. That is the single
most important choice in this file: re-implementing presence semantics in
Spark SQL would fork the one piece of logic every downstream layer inherits,
and invariant 3.1 would then hold in one code path and not the other.
"""

from __future__ import annotations

import argparse
import gzip
import os
import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from spark.schemas import ARRAY_SCHEMAS, SCHEMAS  # noqa: E402
from spark.session import RETENTION_PROPERTIES, build  # noqa: E402

# Archive filenames are "<poll_epoch>-<payload_hash_prefix>.pb.gz".
_FILENAME_RE = re.compile(r"/(\d{10})-([0-9a-f]+)\.pb\.gz$")

PROCESSED_FILES_PATH = "lake/_bronze_processed_files"


def _decode_partition(rows, feed_type: str, poll_interval_s: int):
    """Decode a partition of (path, content) pairs into decoded row dicts.

    Runs on the executor. Imports inside the function because the module must
    be importable in the worker process, not merely in the driver.
    """
    sys.path.insert(0, str(REPO_ROOT))
    from ingest.poller.decode import decode

    for row in rows:
        path = row["path"]
        raw = bytes(row["content"])
        m = _FILENAME_RE.search(path)
        poll_epoch = int(m.group(1)) if m else None

        try:
            payload = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
            decoded, _meta = decode(payload, feed_type, poll_interval_s)
        except Exception as exc:  # noqa: BLE001
            # Invariant 3.9: a decode failure quarantines the payload, it never
            # loses it. The bytes are still in the archive; we surface the
            # failure as a row so it is counted rather than silently skipped.
            yield {
                "source_path": path,
                "poll_epoch": poll_epoch,
                "decode_error": f"{type(exc).__name__}: {exc}",
                "payload": None,
            }
            continue

        for d in decoded:
            # Invariant 3.4: ingest_ts must be when WE received the payload.
            # decode.py stamps datetime.now() at decode time, which for an
            # archive replay is *today*, not the poll. Re-deriving it from the
            # filename keeps event/ingest time honest across reprocessing.
            if poll_epoch is not None:
                d["ingest_ts"] = poll_epoch
            yield {
                "source_path": path,
                "poll_epoch": poll_epoch,
                "decode_error": None,
                "payload": d,
            }


def _already_processed(spark: SparkSession) -> set[str]:
    if not Path(PROCESSED_FILES_PATH).exists():
        return set()
    df = spark.read.format("delta").load(PROCESSED_FILES_PATH)
    return {r["source_path"] for r in df.select("source_path").collect()}


def _record_processed(spark: SparkSession, paths: list[str], feed_type: str) -> None:
    schema = StructType(
        [
            StructField("source_path", StringType(), False),
            StructField("feed_type", StringType(), False),
        ]
    )
    df = spark.createDataFrame([(p, feed_type) for p in paths], schema)
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .save(PROCESSED_FILES_PATH)
    )


def run(
    spark: SparkSession,
    feed_type: str,
    data_root: str = "data",
    lake_root: str = "lake",
    poll_interval_s: int = 120,
    limit_files: int | None = None,
) -> DataFrame:
    src = f"{data_root}/raw/{feed_type}"
    if not Path(src).exists():
        raise FileNotFoundError(f"no archive at {src}")

    files = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.pb.gz")
        .option("recursiveFileLookup", "true")
        .load(src)
        .select("path", "content")
    )

    # File-level idempotence. Bronze is append-only, so re-running must not
    # re-append. Tracking source files (rather than MERGE-ing tens of millions
    # of rows on a natural key) is both cheaper and the standard approach --
    # it is what Auto Loader does under the hood.
    done = _already_processed(spark)
    if done:
        files = files.filter(~F.col("path").isin(list(done)))

    if limit_files:
        files = files.limit(limit_files)

    paths = [r["path"] for r in files.select("path").collect()]
    if not paths:
        print(f"  {feed_type}: no new files")
        return spark.createDataFrame([], SCHEMAS[feed_type])

    out_schema = StructType(
        [
            StructField("source_path", StringType(), False),
            StructField("poll_epoch", LongType(), True),
            StructField("decode_error", StringType(), True),
            StructField("payload", SCHEMAS[feed_type], True),
        ]
    )

    decoded = files.rdd.mapPartitions(
        lambda rows: _decode_partition(rows, feed_type, poll_interval_s)
    ).toDF(out_schema)
    decoded.cache()

    failures = decoded.filter(F.col("decode_error").isNotNull())
    n_fail = failures.count()
    if n_fail:
        (
            failures.select("source_path", "poll_epoch", "decode_error")
            .write.format("delta")
            .mode("append")
            .save(f"{lake_root}/bronze/_decode_failures")
        )
        print(f"  {feed_type}: {n_fail} payloads quarantined")

    rows = (
        decoded.filter(F.col("decode_error").isNull())
        .select("source_path", F.col("payload.*"))
        # ingest_ts arrived as an epoch long from the executor; render it UTC.
        .withColumn(
            "ingest_ts",
            F.to_timestamp(F.from_unixtime(F.col("ingest_ts").cast("long"))),
        )
        .withColumn("bronze_loaded_at", F.current_timestamp())
    )

    target = f"{lake_root}/bronze/{feed_type}"
    writer = (
        rows.write.format("delta")
        .mode("append")
        # Invariant: schema evolution OFF on write paths. A typo'd column must
        # fail the write, not become a permanently-NULL column.
        .option("mergeSchema", "false")
        # Invariant 3.3: raw partitions on ingest_dt because a payload has no
        # single service date. True service_date partitioning starts HERE,
        # after the explode, where each row carries its own.
        .partitionBy("service_date")
    )
    if not Path(target).exists():
        for k, v in RETENTION_PROPERTIES.items():
            writer = writer.option(k, v)
    writer.save(target)

    n = rows.count()
    _record_processed(spark, paths, feed_type)
    decoded.unpersist()
    print(f"  {feed_type}: {len(paths)} files -> {n:,} rows")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", choices=["trip_updates", "vehicle_positions", "both"], default="both")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--lake-root", default="lake")
    ap.add_argument("--poll-interval-s", type=int, default=120)
    ap.add_argument("--limit-files", type=int, default=None)
    args = ap.parse_args()

    spark = build("bronze")
    feeds = ["trip_updates", "vehicle_positions"] if args.feed == "both" else [args.feed]
    print("BRONZE")
    for feed in feeds:
        run(
            spark,
            feed,
            data_root=args.data_root,
            lake_root=args.lake_root,
            poll_interval_s=args.poll_interval_s,
            limit_files=args.limit_files,
        )
    spark.stop()


if __name__ == "__main__":
    main()
