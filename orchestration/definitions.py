"""Dagster asset graph for the transit lakehouse.

**The decision this file exists to encode:** dbt must never run while a static
ingest is mid-flight. The tempting implementation is two schedules with an
offset -- ingest at :00, dbt at :30. That works until the day ingest runs long,
and then dbt reads a half-written dimension and produces marts that are wrong
without being obviously wrong. Nothing errors; the numbers just move.

Modelling the relationship as an ASSET EDGE removes the race entirely. dbt
cannot start until `fct_stop_otp` has materialised, whatever that took. Timing
becomes a consequence of the dependency graph rather than an assumption layered
on top of it.

The graph:

    gtfs_static_snapshot ---> dim_stop_schedule --+
                                                  |
    bronze_vehicle_positions -> silver_vehicle_positions -> fct_stop_arrival
                                                  |               |
                                                  +-------> fct_stop_otp
                                                                  |
                                                             dbt_marts
                                                                  |
                                                            serving_export
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetCheckResult,
    Definitions,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

LAKE = "lake"
JAVA_HOME = "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"


def _ensure_path() -> None:
    """Put the repo on sys.path inside the executing process.

    Dagster runs each step in a forked worker that re-imports this module but
    does not inherit a parent's sys.path mutation, so a top-level insert is not
    enough -- it fails only at step execution, as `No module named 'spark'`.
    """
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))


def _spark(name: str, **kw):
    _ensure_path()
    from spark.session import build

    return build(name, **kw)


# --- ingest -------------------------------------------------------------


@asset(group_name="ingest", description="Raw 511 GTFS-Static zip, archived and extracted.")
def gtfs_static_snapshot() -> Output[str]:
    _ensure_path()
    from ingest.static.fetch_static import archive, extract, fetch
    import os

    key = os.environ.get("API_511_KEY") or next(
        (l.split("=", 1)[1].strip() for l in (REPO / ".env").read_text().splitlines()
         if l.startswith("API_511_KEY=")), None
    )
    if not key:
        raise RuntimeError("API_511_KEY not available")

    path = archive(fetch(key), str(REPO / "data/static"))
    dest = extract(path)
    return Output(
        str(dest),
        metadata={"zip": str(path), "extracted": str(dest),
                  "note": MetadataValue.text(
                      "Content-addressed: an unchanged feed re-archives to the "
                      "same path and opens no new dimension version.")},
    )


@asset(group_name="bronze", description="Archive -> Delta bronze. Decode + explode only.")
def bronze_vehicle_positions() -> Output[int]:
    _ensure_path()
    from spark.bronze import run

    spark = _spark("dagster-bronze")
    try:
        df = run(spark, "vehicle_positions", lake_root=LAKE)
        n = df.count()
    finally:
        spark.stop()
    return Output(n, metadata={"rows_appended": n})


@asset(
    deps=[bronze_vehicle_positions],
    group_name="silver",
    description="Typed, deduplicated, quality-flagged.",
)
def silver_vehicle_positions() -> Output[int]:
    _ensure_path()
    from spark.silver import run

    spark = _spark("dagster-silver")
    try:
        n = run(spark, "vehicle_positions", lake_root=LAKE).count()
    finally:
        spark.stop()
    return Output(n, metadata={"rows": n})


# --- dimension ----------------------------------------------------------


@asset(
    deps=[gtfs_static_snapshot],
    group_name="dimension",
    description="GTFS CSV -> Delta staging. No versioning; dbt owns Type-2 history.",
)
def gtfs_stop_schedule_staging() -> Output[dict]:
    _ensure_path()
    from spark.stage_gtfs import run

    cands = sorted((REPO / "data/static/extracted").glob("*"))
    if not cands:
        raise RuntimeError("no extracted GTFS-Static")

    spark = _spark("dagster-stage", shuffle_partitions=64, driver_memory="6g")
    try:
        stats = run(spark, str(cands[-1]), LAKE)
    finally:
        spark.stop()
    return Output(stats, metadata={k: str(v) for k, v in stats.items()})


@asset(
    deps=[gtfs_stop_schedule_staging],
    group_name="dimension",
    description="SCD2 schedule dimension, via `dbt snapshot` (strategy=check).",
)
def dim_stop_schedule() -> Output[str]:
    """Type-2 history is dbt's job, not a hand-rolled MERGE.

    strategy='check' over schedule-relevant columns only: a GTFS zip differing
    in a shape file must not open a new version of all 3.8M stop times.
    """
    import os

    env = {**os.environ, "JAVA_HOME": JAVA_HOME}
    r = subprocess.run(
        [str(REPO / ".venv-spark/bin/dbt"), "snapshot",
         "--project-dir", "dbt", "--profiles-dir", "dbt"],
        cwd=REPO, env=env, capture_output=True, text=True,
    )
    tail = "\n".join(r.stdout.strip().splitlines()[-8:])
    if r.returncode != 0:
        raise RuntimeError(f"dbt snapshot failed:\n{tail}")
    return Output("ok", metadata={"dbt_tail": MetadataValue.md(f"```\n{tail}\n```")})


# --- facts --------------------------------------------------------------


@asset(
    deps=[silver_vehicle_positions],
    group_name="gold",
    description="Derived arrivals, resolver A, idempotent MERGE on the grain.",
)
def fct_stop_arrival() -> Output[dict]:
    _ensure_path()
    from spark.gold import merge, resolve_arrivals

    spark = _spark("dagster-gold")
    try:
        arrivals = resolve_arrivals(spark, LAKE)
        stats = merge(spark, arrivals, LAKE)
    finally:
        spark.stop()
    return Output(stats, metadata={k: str(v) for k, v in stats.items()})


@asset(
    deps=[fct_stop_arrival, dim_stop_schedule],
    group_name="gold",
    description="As-of join of arrivals to the schedule in force. True OTP.",
)
def fct_stop_otp() -> Output[dict]:
    """Both upstreams are declared, so this cannot run against a partially
    written dimension. That edge is the whole point of the file."""
    _ensure_path()
    from spark.fct_otp import run

    spark = _spark("dagster-otp", shuffle_partitions=32, driver_memory="6g")
    try:
        stats = run(spark, LAKE)
    finally:
        spark.stop()
    return Output(stats, metadata={k: str(v) for k, v in stats.items()})


# --- marts + serving ----------------------------------------------------


@asset(
    deps=[fct_stop_otp],
    group_name="marts",
    description="dbt models. Runs only after fct_stop_otp materialises.",
)
def dbt_marts() -> Output[str]:
    import os

    env = {**os.environ, "JAVA_HOME": JAVA_HOME}
    r = subprocess.run(
        [str(REPO / ".venv-spark/bin/dbt"), "build",
         "--project-dir", "dbt", "--profiles-dir", "dbt"],
        cwd=REPO, env=env, capture_output=True, text=True,
    )
    tail = "\n".join(r.stdout.strip().splitlines()[-12:])
    if r.returncode != 0:
        raise RuntimeError(f"dbt build failed:\n{tail}")
    return Output("ok", metadata={"dbt_tail": MetadataValue.md(f"```\n{tail}\n```")})


@asset(
    deps=[dbt_marts],
    group_name="serving",
    description="Compact SQLite for the always-on API.",
)
def serving_export() -> Output[int]:
    import os

    env = {**os.environ, "JAVA_HOME": JAVA_HOME}
    r = subprocess.run(
        [str(REPO / ".venv-spark/bin/python"), "-m", "serving.export_marts"],
        cwd=REPO, env=env, capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-2000:])
    db = REPO / "serving/data/marts.sqlite"
    return Output(db.stat().st_size, metadata={"bytes": db.stat().st_size})


# --- asset checks: the invariant harness, section 8 ---------------------


@asset_check(asset=fct_stop_arrival, description="Grain uniqueness on the gold grain.")
def check_grain_unique() -> AssetCheckResult:
    spark = _spark("check-grain")
    try:
        df = spark.read.format("delta").load(f"{LAKE}/gold/fct_stop_arrival")
        n, k = df.count(), df.select("service_date", "trip_id", "stop_sequence").distinct().count()
    finally:
        spark.stop()
    return AssetCheckResult(passed=(n == k), metadata={"rows": n, "distinct_keys": k})


@asset_check(asset=fct_stop_otp, description="Join conservation across the as-of join.")
def check_join_conservation() -> AssetCheckResult:
    """A LEFT join that changes the row count means a duplicate dimension
    version matched -- the failure the SCD2 one-current-version test guards."""
    spark = _spark("check-join")
    try:
        f = spark.read.format("delta").load(f"{LAKE}/gold/fct_stop_arrival").count()
        o = spark.read.format("delta").load(f"{LAKE}/gold/fct_stop_otp").count()
    finally:
        spark.stop()
    return AssetCheckResult(passed=(f == o), metadata={"facts": f, "otp_rows": o})


@asset_check(asset=dim_stop_schedule, description="Exactly one current version per key.")
def check_one_current_version() -> AssetCheckResult:
    from pyspark.sql import functions as F

    spark = _spark("check-scd2", shuffle_partitions=32, driver_memory="6g")
    try:
        dim = spark.read.format("delta").load("spark-warehouse/transit.db/dim_stop_schedule")
        # dbt marks the open version with dbt_valid_to IS NULL.
        cur = dim.filter(F.col("dbt_valid_to").isNull())
        n, k = cur.count(), cur.select("schedule_key").distinct().count()
    finally:
        spark.stop()
    return AssetCheckResult(passed=(n == k), metadata={"current_rows": n, "distinct_keys": k})


# --- jobs + schedule ----------------------------------------------------

daily_refresh = define_asset_job("daily_refresh", selection="*")

defs = Definitions(
    assets=[
        gtfs_static_snapshot,
        gtfs_stop_schedule_staging,
        bronze_vehicle_positions,
        silver_vehicle_positions,
        dim_stop_schedule,
        fct_stop_arrival,
        fct_stop_otp,
        dbt_marts,
        serving_export,
    ],
    asset_checks=[check_grain_unique, check_join_conservation, check_one_current_version],
    jobs=[daily_refresh],
    schedules=[
        # ONE schedule for the whole graph. Ordering inside it is the
        # dependency graph's job, not the scheduler's.
        ScheduleDefinition(job=daily_refresh, cron_schedule="0 4 * * *"),
    ],
)
