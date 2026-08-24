"""Read-only FastAPI over the exported marts.

No Spark, no JVM, no Delta. This process opens a SQLite file, which is what
lets the public demo stay always-on and cheap while the heavy infrastructure
stays ephemeral (CLAUDE.md section 7).

Every response carries freshness metadata. The dashboard is required to label
its own staleness, and the API is where that obligation starts: a client cannot
label what the server does not tell it.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DB = Path(__file__).parent / "data" / "marts.sqlite"
STATIC = Path(__file__).parent / "static"

# Beyond this the dashboard shows a stale banner rather than pretending.
STALE_AFTER_HOURS = 36

app = FastAPI(
    title="Transit Reliability API",
    description=(
        "On-time performance derived from GTFS-Realtime vehicle positions. "
        "Data provided by 511.org (MTC), http://www.511.org"
    ),
    version="1.0.0",
)


def _con() -> sqlite3.Connection:
    if not DB.exists():
        raise HTTPException(
            503, "marts not exported yet -- run `make serve-export`"
        )
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def _meta(con: sqlite3.Connection) -> dict:
    return {r["key"]: json.loads(r["value"]) for r in con.execute("SELECT * FROM meta")}


def _freshness(meta: dict) -> dict:
    """Compute staleness server-side so every client agrees on it."""
    last = meta.get("last_arrival_utc")
    try:
        dt = datetime.fromisoformat(str(last)).replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:  # noqa: BLE001
        return {"is_stale": True, "age_hours": None, "reason": "unparseable window"}
    return {
        "is_stale": age_h > STALE_AFTER_HOURS,
        "age_hours": round(age_h, 1),
        "stale_after_hours": STALE_AFTER_HOURS,
        "last_arrival_utc": last,
    }


@app.get("/health")
def health() -> dict:
    if not DB.exists():
        return {"status": "degraded", "reason": "no marts exported"}
    with _con() as con:
        return {"status": "ok", "freshness": _freshness(_meta(con))}


@app.get("/api/meta")
def meta() -> dict:
    with _con() as con:
        m = _meta(con)
        m["freshness"] = _freshness(m)
        return m


@app.get("/api/agencies")
def agencies() -> dict:
    with _con() as con:
        rows = [
            dict(r)
            for r in con.execute(
                "SELECT * FROM mart_otp_by_agency ORDER BY on_time_pct DESC"
            )
        ]
        return {"freshness": _freshness(_meta(con)), "agencies": rows}


@app.get("/api/worst-routes")
def worst_routes(limit: int = Query(20, ge=1, le=200), agency: str | None = None) -> dict:
    sql = "SELECT * FROM mart_worst_routes"
    params: list = []
    if agency:
        sql += " WHERE agency = ?"
        params.append(agency)
    sql += " ORDER BY worst_rank LIMIT ?"
    params.append(limit)
    with _con() as con:
        return {
            "freshness": _freshness(_meta(con)),
            "routes": [dict(r) for r in con.execute(sql, params)],
        }


@app.get("/api/otp-by-hour")
def otp_by_hour(agency: str | None = None) -> dict:
    """OTP by LOCAL hour. The conversion happened once, upstream in dbt."""
    sql = (
        "SELECT hour_local, SUM(n_arrivals) AS n_arrivals, "
        "ROUND(SUM(on_time_pct * n_arrivals) / SUM(n_arrivals), 1) AS on_time_pct "
        "FROM mart_otp_by_route_hour"
    )
    params: list = []
    if agency:
        sql += " WHERE agency = ?"
        params.append(agency)
    sql += " GROUP BY hour_local ORDER BY hour_local"
    with _con() as con:
        return {
            "freshness": _freshness(_meta(con)),
            "hours": [dict(r) for r in con.execute(sql, params)],
        }


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")
