"""Paths, source contract, and quality thresholds for the TLC lakehouse."""
from __future__ import annotations

import os
from pathlib import Path

# Project root is resolved from this file so the DAG works no matter what
# AIRFLOW_HOME is set to, and can still be overridden for tests.
PROJECT_ROOT = Path(os.environ.get("TLC_PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA_ROOT = PROJECT_ROOT / "data"

BRONZE = DATA_ROOT / "bronze"
SILVER = DATA_ROOT / "silver"
QUARANTINE = DATA_ROOT / "quarantine"
GOLD = DATA_ROOT / "gold"
WAREHOUSE = GOLD / "warehouse.duckdb"

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# The source publishes roughly two months in arrears. Runs before the file
# exists should wait rather than fail.
SOURCE_LAG_MONTHS = 2

# --------------------------------------------------------------------------
# Schema contract. The pipeline refuses to promote a file whose shape drifted
# from what the transforms assume, instead of silently producing null columns.
# --------------------------------------------------------------------------
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "VendorID": ("INTEGER", "BIGINT", "INT32", "INT64"),
    "tpep_pickup_datetime": ("TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_US"),
    "tpep_dropoff_datetime": ("TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_US"),
    "passenger_count": ("DOUBLE", "BIGINT", "INTEGER", "FLOAT"),
    "trip_distance": ("DOUBLE", "FLOAT"),
    "PULocationID": ("INTEGER", "BIGINT", "INT32", "INT64"),
    "DOLocationID": ("INTEGER", "BIGINT", "INT32", "INT64"),
    "payment_type": ("BIGINT", "INTEGER", "INT64"),
    "fare_amount": ("DOUBLE", "FLOAT"),
    "tip_amount": ("DOUBLE", "FLOAT"),
    "total_amount": ("DOUBLE", "FLOAT"),
}

# --------------------------------------------------------------------------
# Quality gate thresholds. Breaching a hard threshold fails the run and stops
# the partition from reaching gold; a soft breach is logged and continues.
# --------------------------------------------------------------------------
MAX_REJECT_RATE = 0.05          # hard: >5% of rows unusable means bad source
WARN_REJECT_RATE = 0.01         # soft: worth looking at
MIN_ROWS_PER_MONTH = 500_000    # hard: a real month is millions of trips
MAX_NULL_RATE_KEY_COLS = 0.001  # hard: keys must be present

# Plausibility bounds, applied per row in silver.
MAX_TRIP_MILES = 500.0
MAX_TRIP_HOURS = 12.0
MAX_FARE = 5_000.0
MAX_PASSENGERS = 9
VALID_ZONE_IDS = (1, 265)


def partition_path(root: Path, year: int, month: int) -> Path:
    """Hive-style partition directory: root/year=2024/month=01."""
    return root / f"year={year:04d}" / f"month={month:02d}"


def ensure_dirs() -> None:
    for d in (BRONZE, SILVER, QUARANTINE, GOLD):
        d.mkdir(parents=True, exist_ok=True)
