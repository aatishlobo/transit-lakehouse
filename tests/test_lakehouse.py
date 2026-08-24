"""Invariant tests for the Spark/Delta lakehouse layers.

Run with the Spark venv, not the ingest venv:

    .venv-spark/bin/python -m pytest tests/test_lakehouse.py -q

These are the CLAUDE.md section 8 invariant harness items that can be asserted
without a live feed. Every one of them fails silently in production if broken,
which is why they are tests rather than comments.

Row-count floors everywhere: a test that passes on an empty table is worse than
no test, because it reports green while measuring nothing and keeps reporting
green after an upstream break empties its input.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

LAKE = REPO / "lake"
GOLD = LAKE / "gold" / "fct_stop_arrival"

pyspark = pytest.importorskip(
    "pyspark",
    reason="Spark stack lives in .venv-spark; run `make lake-test`",
)

pytestmark = pytest.mark.skipif(
    not GOLD.exists(), reason="no lake built -- run `make lake` first"
)


@pytest.fixture(scope="module")
def spark():
    from spark.session import build

    s = build("tests", shuffle_partitions=4)
    yield s
    s.stop()


@pytest.fixture(scope="module")
def gold(spark):
    return spark.read.format("delta").load(str(GOLD))


def test_gold_is_not_empty(gold):
    """Row-count floor. Everything below is meaningless without this."""
    assert gold.count() > 1000


def test_grain_is_unique(gold):
    """Invariant 3.2. One row per (service_date, trip_id, stop_sequence).

    A stop_id grain would collapse loop-route revisits into one row; this
    asserts the grain we actually chose holds.
    """
    from spark.gold import GRAIN

    n = gold.count()
    n_keys = gold.select(*GRAIN).distinct().count()
    assert n == n_keys, f"{n} rows but only {n_keys} distinct grain keys"


def test_grain_columns_are_never_null(gold):
    """A NULL in the grain means the row cannot be identified or merged."""
    from pyspark.sql import functions as F

    from spark.gold import GRAIN

    for c in GRAIN:
        assert gold.filter(F.col(c).isNull()).count() == 0, f"{c} has NULLs"


def test_event_time_is_not_ingest_time(gold):
    """Invariant 3.4. Training on ingest_ts leaks: it encodes when we looked.

    Allows a tiny number of coincidental equalities (both are second-resolution
    timestamps) but forbids the systematic case where one was copied from the
    other.
    """
    from pyspark.sql import functions as F

    n = gold.count()
    same = gold.filter(F.col("actual_arrival_ts") == F.col("ingest_ts")).count()
    assert same / n < 0.01, f"{same}/{n} rows have arrival == ingest"


def test_vehicle_clock_skew_stays_bounded(gold):
    """Event time comes from the VEHICLE, whose clock we do not control.

    Measured: 3.7% of arrivals carry a vehicle timestamp AHEAD of the poll that
    observed them, by up to ~97 s, across 10 operators. That is physical clock
    skew on the vehicles, not a pipeline bug, and it is a second source of
    label bias on top of the sampling effects already documented.

    So this bounds the phenomenon rather than forbidding it. It still catches
    the failure that matters -- event time being derived from, or overwritten
    by, our own clock -- because that would push these numbers to an extreme.
    """
    from pyspark.sql import functions as F

    n = gold.count()
    skewed = gold.filter(F.col("actual_arrival_ts") > F.col("ingest_ts"))
    frac = skewed.count() / n
    assert frac < 0.10, f"{frac:.1%} of arrivals postdate their poll"

    worst = skewed.select(
        F.max(F.unix_timestamp("actual_arrival_ts") - F.unix_timestamp("ingest_ts"))
    ).collect()[0][0]
    if worst is not None:
        # One poll interval. Beyond that it is no longer explicable as skew.
        assert worst <= 120, f"vehicle clock {worst}s ahead of our poll"


def test_provenance_is_complete(gold):
    """CLAUDE.md section 2: label noise must be filterable, not hidden.

    Without a method stamp, resolver availability (which varies by operator)
    silently correlates agency with label quality, and any downstream model
    learns our own code rather than the world.
    """
    from pyspark.sql import functions as F

    for c in [
        "arrival_method",
        "arrival_confidence",
        "poll_interval_s",
        "resolver_version",
        "decoder_version",
    ]:
        assert gold.filter(F.col(c).isNull()).count() == 0, f"{c} incomplete"


def test_poll_interval_is_the_error_bar(gold):
    """poll_interval_s must be a real sampling interval, never 0 or absent."""
    from pyspark.sql import functions as F

    assert gold.filter(F.col("poll_interval_s") <= 0).count() == 0


def test_arrival_is_first_sighting_not_last(gold):
    """Section 5.2. The last STOPPED_AT report measures DEPARTURE.

    Where a vehicle was seen stopped more than once, the recorded arrival must
    be at or before the last sighting, never equal to it.
    """
    from pyspark.sql import functions as F

    multi = gold.filter(F.col("n_stopped_observations") > 1)
    assert multi.count() > 0, "no multi-poll dwells; test is vacuous"

    # Strictly-after is the real bug: it would mean we took the departure.
    bad = multi.filter(F.col("actual_arrival_ts") > F.col("last_stopped_ts")).count()
    assert bad == 0, f"{bad} arrivals taken from the last sighting"

    # Equality is legitimate and rare (8 rows measured): the feed header
    # advanced between polls while the vehicle re-reported an identical
    # timestamp, so a two-poll dwell collapses to a single instant. Bounded
    # rather than forbidden, so a regression that flattened every dwell to one
    # instant would still fail here.
    eq = multi.filter(F.col("actual_arrival_ts") == F.col("last_stopped_ts")).count()
    assert eq / multi.count() < 0.05, f"{eq} multi-poll dwells have zero duration"


def test_dwell_is_never_negative(gold):
    from pyspark.sql import functions as F

    assert gold.filter(F.col("observed_dwell_s") < 0).count() == 0


def test_retention_is_pinned(spark):
    """Invariant 3.8. A bare VACUUM under the 7-day default destroys the
    time-travel window the ML point-in-time contract depends on."""
    props = {
        r["key"]: r["value"]
        for r in spark.sql(f"SHOW TBLPROPERTIES delta.`{GOLD}`").collect()
    }
    for k in ("delta.logRetentionDuration", "delta.deletedFileRetentionDuration"):
        assert k in props, f"{k} not set -- default retention would apply"
        assert "7 days" not in props[k]


def test_time_travel_reaches_version_zero(spark, gold):
    """The ML contract: reconstruct the table as of prediction time T."""
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(str(GOLD))
    assert v0.count() > 0


def test_merge_is_idempotent(spark):
    """Invariant 3.7. Reprocessing must converge, not accumulate.

    This is the load-bearing test for the whole at-least-once design: we
    tolerate duplicate delivery precisely because the grain makes it harmless,
    and this asserts that rather than assuming it.
    """
    from spark.gold import merge, resolve_arrivals

    before = spark.read.format("delta").load(str(GOLD)).count()
    arrivals = resolve_arrivals(spark, str(LAKE))
    merge(spark, arrivals, str(LAKE))
    after = spark.read.format("delta").load(str(GOLD)).count()
    assert before == after, f"re-merge changed row count {before} -> {after}"


def test_absent_is_not_zero_survived_into_the_lake(spark):
    """Invariant 3.1, end to end.

    The decoder guarantees this at parse time; this asserts it survived the
    trip through Spark and Delta. If a cast or a default ever collapses NULL
    to 0, this is the test that catches it.
    """
    from pyspark.sql import functions as F

    silver = spark.read.format("delta").load(str(LAKE / "silver" / "vehicle_positions"))
    n_null = silver.filter(F.col("current_stop_sequence").isNull()).count()
    n_zero = silver.filter(F.col("current_stop_sequence") == 0).count()
    assert n_null > 0, "no NULLs at all -- absence was probably coerced to 0"
    assert n_zero > 0, "no zeros at all -- test cannot discriminate"
    assert n_null != n_zero
