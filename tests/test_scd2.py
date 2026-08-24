"""SCD2 behaviour of dim_stop_schedule.

Uses a tiny synthetic GTFS snapshot rather than the 275 MB real feed: the
property under test is versioning semantics, and a test that takes four minutes
does not get run.

The failure this guards against is the quiet one. If the dimension overwrites
instead of versioning, every query still works, every row count looks sane, and
every historical OTP number silently changes the next time an agency publishes
a timetable.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

pytest.importorskip("pyspark", reason="Spark stack lives in .venv-spark")

STOP_TIMES_HEADER = (
    "trip_id,stop_id,location_group_id,location_id,stop_sequence,stop_headsign,"
    "arrival_time,departure_time,pickup_type,drop_off_type,timepoint\n"
)
TRIPS_HEADER = "route_id,service_id,trip_id,trip_headsign,direction_id\n"


def _write_snapshot(root: Path, name: str, first_arrival: str) -> Path:
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "stop_times.txt").write_text(
        STOP_TIMES_HEADER
        # Deliberately includes a past-midnight time and a loop-route revisit
        # of the same stop_id at two different stop_sequences.
        + f"AG:T1,100,,,1,,{first_arrival},{first_arrival},0,0,1\n"
        + "AG:T1,200,,,2,,25:14:00,25:15:00,0,0,0\n"
        + "AG:T1,100,,,3,,25:40:00,25:41:00,0,0,1\n"
    )
    (d / "trips.txt").write_text(TRIPS_HEADER + "AG:R1,AG:S1,AG:T1,Loop,0\n")
    return d


@pytest.fixture(scope="module")
def spark():
    from spark.session import build

    s = build("scd2-tests", shuffle_partitions=2, driver_memory="2g")
    yield s
    s.stop()


@pytest.fixture
def lake(tmp_path):
    return str(tmp_path / "lake")


def _dim(spark, lake):
    return spark.read.format("delta").load(f"{lake}/dim/dim_stop_schedule")


def test_first_load_opens_one_version_per_key(spark, tmp_path, lake):
    from spark.dim_schedule import run

    snap = _write_snapshot(tmp_path, "v1", "08:00:00")
    stats = run(spark, str(snap), lake, observed_at="2026-08-01 00:00:00")

    assert stats["snapshot_rows"] == 3
    assert stats["orphan_stop_times"] == 0
    df = _dim(spark, lake)
    assert df.count() == 3
    assert df.filter("is_current").count() == 3


def test_loop_route_revisit_is_two_rows_not_one(spark, tmp_path, lake):
    """Invariant 3.2. stop_id 100 is served at stop_sequence 1 AND 3."""
    from spark.dim_schedule import run

    run(spark, str(_write_snapshot(tmp_path, "v1", "08:00:00")), lake,
        observed_at="2026-08-01 00:00:00")
    df = _dim(spark, lake)
    assert df.filter("stop_id = 100").count() == 2
    assert df.filter("stop_id = 100").select("stop_sequence").distinct().count() == 2


def test_unchanged_reload_opens_no_new_versions(spark, tmp_path, lake):
    """Re-ingesting an identical schedule must be a no-op.

    A dimension that opens a version per run reports the timetable as changing
    daily when nothing changed, and the as-of join slows down for no signal.
    """
    from spark.dim_schedule import run

    snap = _write_snapshot(tmp_path, "v1", "08:00:00")
    run(spark, str(snap), lake, observed_at="2026-08-01 00:00:00")
    stats = run(spark, str(snap), lake, observed_at="2026-08-02 00:00:00")

    assert stats["closed"] == 0 and stats["opened"] == 0
    assert _dim(spark, lake).count() == 3


def test_changed_time_closes_old_and_opens_new(spark, tmp_path, lake):
    from spark.dim_schedule import run
    from pyspark.sql import functions as F

    run(spark, str(_write_snapshot(tmp_path, "v1", "08:00:00")), lake,
        observed_at="2026-08-01 00:00:00")
    stats = run(spark, str(_write_snapshot(tmp_path, "v2", "08:05:00")), lake,
                observed_at="2026-08-10 00:00:00")

    assert stats["closed"] == 1, "the changed key must close its old version"
    assert stats["opened"] == 1

    df = _dim(spark, lake)
    assert df.count() == 4, "3 keys, one of which now has two versions"

    hist = df.filter("stop_sequence = 1").orderBy("valid_from").collect()
    assert len(hist) == 2
    old, new = hist
    assert old["arrival_time"] == "08:00:00" and not old["is_current"]
    assert new["arrival_time"] == "08:05:00" and new["is_current"]

    # Windows must TILE: no gap and no overlap, or an as-of join returns
    # either zero rows or two for an instant in between.
    assert str(old["valid_to"]) == str(new["valid_from"])

    # The untouched keys keep exactly one open version.
    assert df.filter("stop_sequence = 2 AND is_current").count() == 1


def test_new_key_inserts_without_closing_anything(spark, tmp_path, lake):
    from spark.dim_schedule import run

    run(spark, str(_write_snapshot(tmp_path, "v1", "08:00:00")), lake,
        observed_at="2026-08-01 00:00:00")

    d = _write_snapshot(tmp_path, "v3", "08:00:00")
    with (d / "stop_times.txt").open("a") as fh:
        fh.write("AG:T1,300,,,4,,26:00:00,26:01:00,0,0,0\n")

    stats = run(spark, str(d), lake, observed_at="2026-08-11 00:00:00")
    assert stats["closed"] == 0 and stats["opened"] == 1
    df = _dim(spark, lake)
    assert df.count() == 4 and df.filter("is_current").count() == 4


def test_exactly_one_current_version_per_key(spark, tmp_path, lake):
    """The invariant every as-of join depends on. Two open versions for one key
    silently doubles fact rows on join -- join conservation fails."""
    from spark.dim_schedule import run

    run(spark, str(_write_snapshot(tmp_path, "v1", "08:00:00")), lake,
        observed_at="2026-08-01 00:00:00")
    run(spark, str(_write_snapshot(tmp_path, "v2", "08:05:00")), lake,
        observed_at="2026-08-10 00:00:00")
    run(spark, str(_write_snapshot(tmp_path, "v4", "08:09:00")), lake,
        observed_at="2026-08-20 00:00:00")

    df = _dim(spark, lake).filter("is_current")
    assert df.count() == df.select("trip_id", "stop_sequence").distinct().count()
    assert _dim(spark, lake).filter("stop_sequence = 1").count() == 3
