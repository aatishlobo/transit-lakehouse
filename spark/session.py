"""SparkSession construction for the lakehouse jobs.

Every configuration here encodes an invariant from CLAUDE.md. None of them are
defaults worth inheriting, and several are silent-corruption traps if left off.
"""

from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Delta table properties applied at creation. Pitfall 3.8: Delta's default
# deletedFileRetentionDuration is 7 days, and a bare VACUUM enforces it. The ML
# contract (CLAUDE.md 7.2) requires time travel across the whole training
# window, so a default VACUUM would destroy the single most valuable
# correctness property in the portfolio. Set explicitly, and pay the storage.
RETENTION_PROPERTIES = {
    "delta.logRetentionDuration": "interval 90 days",
    "delta.deletedFileRetentionDuration": "interval 90 days",
}


def build(
    app_name: str,
    shuffle_partitions: int = 8,
    driver_memory: str | None = None,
) -> SparkSession:
    """Local SparkSession with Delta enabled.

    shuffle_partitions defaults low because this runs on one laptop against a
    ~1 GB archive; the 200 default produces hundreds of tiny files and spends
    more time in task scheduling than in work.
    """
    # Pin the interpreter on BOTH sides. Without this the driver runs the venv
    # (3.12) while executors inherit whatever `python3` resolves to on PATH
    # (3.13 here), and PySpark refuses to run across minor versions. In local
    # mode the failure surfaces only once a Python UDF is evaluated, i.e. deep
    # inside a job rather than at session construction.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # In local mode the driver IS the executor, so the 1 GB default heap has to
    # hold every shuffle. It must be set before the JVM launches -- passing it
    # to an already-running SparkContext is silently ignored, which is why an
    # OOM here looks like a code problem rather than a config one.
    mem = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY", "4g")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # --- Delta -------------------------------------------------------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # --- Invariant 3.5: all timestamps stored UTC --------------------
        # Converting to America/Los_Angeles happens exactly once, in serving.
        # Truncating hour-of-day in local time here would shift every bar of
        # an OTP-by-hour chart by 7-8 hours while leaving totals correct: the
        # most user-visible possible bug in this project.
        .config("spark.sql.session.timeZone", "UTC")
        # --- Schema evolution OFF on write paths -------------------------
        # mergeSchema silently accepts a typo'd column as a new column, which
        # then reads as all-NULL forever. Explicit schemas only.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        # Fail the write rather than silently dropping rows whose partition
        # value is unexpected.
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", mem)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        # Parsing legacy/ambiguous datetimes should raise, not guess.
        .config("spark.sql.legacy.timeParserPolicy", "EXCEPTION")
    )
    # The `delta-spark` pip package ships only the Python API; the JVM classes
    # arrive as a Maven coordinate. Without this, spark.sql.extensions above
    # raises ClassNotFoundException at first Delta touch.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def apply_retention(spark: SparkSession, table_path: str) -> None:
    """Pin retention on an existing Delta table. Idempotent."""
    props = ", ".join(f"'{k}' = '{v}'" for k, v in RETENTION_PROPERTIES.items())
    spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES ({props})")
