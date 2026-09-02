"""Silver layer: enforce the schema contract, then split clean from dirty.

Every source row lands in exactly one of two places, silver or quarantine, so
row counts always reconcile. Nothing is dropped silently.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import duckdb

from . import config

log = logging.getLogger(__name__)

# Ordered: the first matching predicate wins, so a row is attributed to its
# most fundamental problem rather than to whichever check ran last.
REJECTION_RULES = """
CASE
  WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
       THEN 'null_timestamp'
  WHEN tpep_dropoff_datetime <= tpep_pickup_datetime
       THEN 'non_positive_duration'
  WHEN tpep_pickup_datetime < $period_start OR tpep_pickup_datetime >= $period_end
       THEN 'outside_partition_period'
  WHEN date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > $max_minutes
       THEN 'implausible_duration'
  WHEN trip_distance IS NULL OR trip_distance <= 0
       THEN 'non_positive_distance'
  WHEN trip_distance > $max_miles
       THEN 'implausible_distance'
  WHEN fare_amount < 0 OR total_amount < 0
       THEN 'negative_amount'
  WHEN total_amount > $max_fare
       THEN 'implausible_fare'
  WHEN PULocationID NOT BETWEEN $zone_lo AND $zone_hi
       OR DOLocationID NOT BETWEEN $zone_lo AND $zone_hi
       THEN 'unknown_zone'
  WHEN passenger_count > $max_pax
       THEN 'implausible_passenger_count'
  ELSE NULL
END
"""


class SchemaContractError(Exception):
    """Source columns drifted from what the transforms assume."""


def check_contract(con: duckdb.DuckDBPyConnection, src: str) -> dict[str, str]:
    """Fail loudly if the source lost a column or changed a type.

    Without this, a renamed upstream column produces a silver table full of
    nulls that passes every row-level check.
    """
    actual = {
        r[0]: r[1].upper()
        for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{src}')").fetchall()
    }
    missing = [c for c in config.REQUIRED_COLUMNS if c not in actual]
    if missing:
        raise SchemaContractError(f"source is missing required columns: {missing}")

    wrong = {
        col: actual[col]
        for col, allowed in config.REQUIRED_COLUMNS.items()
        if not any(a in actual[col] for a in allowed)
    }
    if wrong:
        raise SchemaContractError(f"source column types drifted: {wrong}")
    return actual


def transform(year: int, month: int) -> dict:
    """Clean one bronze partition into silver, quarantining the rest."""
    config.ensure_dirs()
    src = config.partition_path(config.BRONZE, year, month) / "trips.parquet"
    if not src.exists():
        raise FileNotFoundError(f"no bronze partition at {src}")

    silver_dir = config.partition_path(config.SILVER, year, month)
    quar_dir = config.partition_path(config.QUARANTINE, year, month)
    silver_dir.mkdir(parents=True, exist_ok=True)
    quar_dir.mkdir(parents=True, exist_ok=True)
    silver_out = silver_dir / "trips.parquet"
    quar_out = quar_dir / "rejects.parquet"

    period_start = datetime(year, month, 1)
    period_end = datetime(year + (month == 12), (month % 12) + 1, 1)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    check_contract(con, str(src))

    params = {
        "period_start": period_start,
        "period_end": period_end,
        "max_minutes": int(config.MAX_TRIP_HOURS * 60),
        "max_miles": config.MAX_TRIP_MILES,
        "max_fare": config.MAX_FARE,
        "zone_lo": config.VALID_ZONE_IDS[0],
        "zone_hi": config.VALID_ZONE_IDS[1],
        "max_pax": config.MAX_PASSENGERS,
    }

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE classified AS
        SELECT *, {REJECTION_RULES} AS rejection_reason
        FROM read_parquet('{src}')
        """,
        params,
    )
    total = con.execute("SELECT count(*) FROM classified").fetchone()[0]

    # Valid rows, typed and enriched. Writing to a temp file then renaming keeps
    # a failed run from leaving a half-written partition for gold to pick up.
    tmp_silver = silver_dir / ".trips.parquet.tmp"
    con.execute(
        f"""
        COPY (
          SELECT
            CAST(VendorID AS SMALLINT)                        AS vendor_id,
            tpep_pickup_datetime                              AS pickup_ts,
            tpep_dropoff_datetime                             AS dropoff_ts,
            CAST(tpep_pickup_datetime AS DATE)                AS pickup_date,
            EXTRACT(hour FROM tpep_pickup_datetime)           AS pickup_hour,
            EXTRACT(dow  FROM tpep_pickup_datetime)           AS pickup_dow,
            CAST(coalesce(passenger_count, 1) AS SMALLINT)    AS passenger_count,
            trip_distance                                     AS trip_miles,
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0
                                                              AS trip_minutes,
            CASE WHEN tpep_dropoff_datetime > tpep_pickup_datetime
                 THEN trip_distance / (date_diff('second', tpep_pickup_datetime,
                                                 tpep_dropoff_datetime) / 3600.0)
            END                                               AS avg_speed_mph,
            CAST(PULocationID AS SMALLINT)                    AS pu_location_id,
            CAST(DOLocationID AS SMALLINT)                    AS do_location_id,
            CAST(payment_type AS SMALLINT)                    AS payment_type,
            fare_amount, tip_amount, total_amount,
            CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount END AS tip_rate,
            PULocationID IN (1, 132, 138)                     AS is_airport_pickup,
            {year}  AS year,
            {month} AS month,
            now()   AS processed_at
          FROM classified
          WHERE rejection_reason IS NULL
        ) TO '{tmp_silver}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp_silver.replace(silver_out)

    tmp_quar = quar_dir / ".rejects.parquet.tmp"
    con.execute(
        f"""
        COPY (
          SELECT *, {year} AS year, {month} AS month, now() AS rejected_at
          FROM classified WHERE rejection_reason IS NOT NULL
        ) TO '{tmp_quar}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp_quar.replace(quar_out)

    breakdown = dict(
        con.execute(
            """
            SELECT rejection_reason, count(*) FROM classified
            WHERE rejection_reason IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall()
    )
    kept = con.execute("SELECT count(*) FROM classified WHERE rejection_reason IS NULL").fetchone()[0]
    con.close()

    rejected = total - kept
    stats = {
        "year": year, "month": month,
        "rows_in": total, "rows_valid": kept, "rows_rejected": rejected,
        "reject_rate": (rejected / total) if total else 0.0,
        "rejection_breakdown": breakdown,
        "silver_path": str(silver_out), "quarantine_path": str(quar_out),
    }
    log.info("silver %04d-%02d: %d in, %d kept, %d quarantined (%.3f%%) %s",
             year, month, total, kept, rejected, stats["reject_rate"] * 100, breakdown)
    return stats
