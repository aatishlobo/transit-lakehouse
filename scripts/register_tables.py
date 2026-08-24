"""Register the Delta tables in the local metastore so dbt can reference them.

dbt models select from `transit.<table>` rather than from a filesystem path:
paths in SQL are unportable and make lineage invisible to dbt. These are
EXTERNAL tables -- dropping one removes the registration, never the data.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from spark.session import build  # noqa: E402

TABLES = {
    "fct_stop_arrival": "lake/gold/fct_stop_arrival",
    "fct_stop_otp": "lake/gold/fct_stop_otp",
    "dim_stop_schedule": "lake/dim/dim_stop_schedule",
}


def main() -> None:
    spark = build("register")
    spark.sql("CREATE DATABASE IF NOT EXISTS transit")
    for name, rel in TABLES.items():
        p = Path(rel).resolve()
        if not p.exists():
            print(f"  skip {name}: {rel} not built")
            continue
        spark.sql(f"DROP TABLE IF EXISTS transit.{name}")
        spark.sql(f"CREATE TABLE transit.{name} USING DELTA LOCATION '{p}'")
        n = spark.table(f"transit.{name}").count()
        print(f"  registered transit.{name}  ({n:,} rows)")
    spark.stop()


if __name__ == "__main__":
    main()
