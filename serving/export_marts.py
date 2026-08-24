"""Export dbt marts to a compact SQLite file for the serving tier.

CLAUDE.md section 7 sets a deliberate tension: the public demo must be
always-on and cheap, while Kafka/Spark/K8s stay ephemeral. That rules out an
API that queries Delta directly -- it would need a live Spark session to
answer a page load.

So the marts are exported to a single SQLite file. The API process then has no
Spark, no JVM, and no Delta dependency; it opens a file. The whole serving tier
can run on the smallest instance available, or on a laptop, indefinitely.

The export stamps `generated_at` and the observed data window. That is not
decoration: the dashboard is required to label its own staleness, because
unlabelled stale data on a live demo reads as a broken product.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.session import build  # noqa: E402

WAREHOUSE = "spark-warehouse/transit.db"
MARTS = ["mart_otp_by_agency", "mart_otp_by_route_hour", "mart_worst_routes"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="serving/data/marts.sqlite")
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()

    spark = build("export", shuffle_partitions=8, driver_memory="4g")
    con = sqlite3.connect(out)

    counts = {}
    for m in MARTS:
        df = spark.read.format("delta").load(f"{WAREHOUSE}/{m}").toPandas()
        df.to_sql(m, con, index=False)
        counts[m] = len(df)
        print(f"  {m}: {len(df):,} rows")

    # Provenance and freshness, read from the fact table rather than assumed.
    facts = spark.read.format("delta").load("lake/gold/fct_stop_otp")
    window = facts.selectExpr(
        "min(actual_arrival_ts) as first_arrival",
        "max(actual_arrival_ts) as last_arrival",
        "count(*) as n_arrivals",
        "count(distinct service_date) as n_service_days",
    ).collect()[0]

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "first_arrival_utc": str(window["first_arrival"]),
        "last_arrival_utc": str(window["last_arrival"]),
        "n_arrivals": int(window["n_arrivals"]),
        "n_service_days": int(window["n_service_days"]),
        "row_counts": counts,
        "arrival_method": "stopped_at (resolver A)",
        "poll_interval_s": 120,
        "known_bias": (
            "Derived arrivals are biased late and over-represent long-dwell "
            "stops; capture is ~21% of stop events. Treat figures as an upper "
            "bound on agency optimism."
        ),
        "attribution": "Data provided by 511.org (MTC) -- http://www.511.org",
    }
    con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    con.executemany(
        "INSERT INTO meta VALUES (?, ?)",
        [(k, json.dumps(v)) for k, v in meta.items()],
    )
    con.commit()
    con.close()
    spark.stop()

    print(f"  meta: window {meta['first_arrival_utc']} -> {meta['last_arrival_utc']}")
    print(f"  wrote {out} ({out.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
