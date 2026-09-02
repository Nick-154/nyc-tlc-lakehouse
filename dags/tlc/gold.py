"""Gold layer: dimensional marts in DuckDB, loaded one partition at a time.

Loads use delete-then-insert inside a transaction, keyed on the partition being
processed. Re-running a month replaces exactly that month's rows and touches
nothing else, which is what makes an arbitrary backfill safe to repeat.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager

import duckdb

from . import config

log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS fct_trips_daily (
    year            SMALLINT NOT NULL,
    month           TINYINT  NOT NULL,
    pickup_date     DATE     NOT NULL,
    pu_location_id  SMALLINT NOT NULL,
    payment_type    SMALLINT,
    trips           BIGINT,
    passengers      BIGINT,
    total_miles     DOUBLE,
    total_fare      DOUBLE,
    total_tips      DOUBLE,
    total_revenue   DOUBLE,
    avg_trip_min    DOUBLE,
    avg_speed_mph   DOUBLE,
    avg_tip_rate    DOUBLE,
    airport_trips   BIGINT,
    loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fct_trips_hourly (
    year          SMALLINT NOT NULL,
    month         TINYINT  NOT NULL,
    pickup_date   DATE     NOT NULL,
    pickup_hour   TINYINT  NOT NULL,
    pickup_dow    TINYINT,
    trips         BIGINT,
    avg_fare      DOUBLE,
    avg_trip_min  DOUBLE,
    avg_speed_mph DOUBLE,
    loaded_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    dag_run_id     VARCHAR,
    year           SMALLINT,
    month          TINYINT,
    rows_in        BIGINT,
    rows_valid     BIGINT,
    rows_rejected  BIGINT,
    reject_rate    DOUBLE,
    gate_passed    BOOLEAN,
    soft_failures  VARCHAR,
    bronze_bytes   BIGINT,
    bronze_sha256  VARCHAR,
    recorded_at    TIMESTAMP
);
"""


@contextmanager
def warehouse(read_only: bool = False):
    config.ensure_dirs()
    con = duckdb.connect(str(config.WAREHOUSE), read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def init_warehouse() -> None:
    with warehouse() as con:
        con.execute(DDL)
    log.info("warehouse initialised at %s", config.WAREHOUSE)


def load_partition(year: int, month: int) -> dict:
    src = config.partition_path(config.SILVER, year, month) / "trips.parquet"
    if not src.exists():
        raise FileNotFoundError(f"no silver partition at {src}")

    with warehouse() as con:
        con.execute(DDL)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"CREATE OR REPLACE TEMP VIEW s AS SELECT * FROM read_parquet('{src}')")

            # Delete-then-insert scoped to this partition only.
            con.execute("DELETE FROM fct_trips_daily WHERE year = ? AND month = ?", [year, month])
            con.execute(
                """
                INSERT INTO fct_trips_daily
                SELECT year, month, pickup_date, pu_location_id, payment_type,
                       count(*), sum(passenger_count), sum(trip_miles),
                       sum(fare_amount), sum(tip_amount), sum(total_amount),
                       avg(trip_minutes), avg(avg_speed_mph), avg(tip_rate),
                       count(*) FILTER (WHERE is_airport_pickup), now()
                FROM s GROUP BY year, month, pickup_date, pu_location_id, payment_type
                """
            )
            daily = con.execute(
                "SELECT count(*) FROM fct_trips_daily WHERE year = ? AND month = ?", [year, month]
            ).fetchone()[0]

            con.execute("DELETE FROM fct_trips_hourly WHERE year = ? AND month = ?", [year, month])
            con.execute(
                """
                INSERT INTO fct_trips_hourly
                SELECT year, month, pickup_date, pickup_hour, any_value(pickup_dow),
                       count(*), avg(fare_amount), avg(trip_minutes), avg(avg_speed_mph), now()
                FROM s GROUP BY year, month, pickup_date, pickup_hour
                """
            )
            hourly = con.execute(
                "SELECT count(*) FROM fct_trips_hourly WHERE year = ? AND month = ?", [year, month]
            ).fetchone()[0]

            trips, revenue = con.execute(
                "SELECT sum(trips), sum(total_revenue) FROM fct_trips_daily WHERE year = ? AND month = ?",
                [year, month],
            ).fetchone()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    stats = {
        "year": year, "month": month,
        "daily_rows": daily, "hourly_rows": hourly,
        "trips": int(trips or 0), "revenue": float(revenue or 0.0),
    }
    log.info("gold %04d-%02d: %d daily rows, %d hourly rows, %d trips, $%.2f revenue",
             year, month, daily, hourly, stats["trips"], stats["revenue"])
    return stats


def record_run(dag_run_id: str, year: int, month: int, silver_stats: dict,
               gate: dict, bronze: dict) -> None:
    with warehouse() as con:
        con.execute(DDL)
        con.execute("DELETE FROM pipeline_runs WHERE dag_run_id = ? AND year = ? AND month = ?",
                    [dag_run_id, year, month])
        con.execute(
            "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?, now())",
            [dag_run_id, year, month,
             silver_stats["rows_in"], silver_stats["rows_valid"], silver_stats["rows_rejected"],
             silver_stats["reject_rate"], gate["passed"], ",".join(gate["soft_failures"]),
             bronze["bytes"], bronze["sha256"]],
        )
