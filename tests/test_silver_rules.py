"""Every source row must land in silver or quarantine, with the right reason."""
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

BASE = dict(
    VendorID=1,
    tpep_pickup_datetime=datetime(2024, 3, 10, 8, 0),
    tpep_dropoff_datetime=datetime(2024, 3, 10, 8, 20),
    passenger_count=1.0,
    trip_distance=3.4,
    PULocationID=100,
    DOLocationID=140,
    payment_type=1,
    fare_amount=18.0,
    tip_amount=3.0,
    total_amount=24.5,
)

# (label, field overrides, expected rejection_reason or None)
CASES = [
    ("clean",                    {},                                                        None),
    ("null_timestamp",           {"tpep_dropoff_datetime": None},                           "null_timestamp"),
    ("dropoff_before_pickup",    {"tpep_dropoff_datetime": datetime(2024, 3, 10, 7, 0)},    "non_positive_duration"),
    ("same_instant",             {"tpep_dropoff_datetime": datetime(2024, 3, 10, 8, 0)},    "non_positive_duration"),
    ("stray_2009_record",        {"tpep_pickup_datetime": datetime(2009, 1, 1),
                                  "tpep_dropoff_datetime": datetime(2009, 1, 1, 0, 20)},    "outside_partition_period"),
    ("next_month",               {"tpep_pickup_datetime": datetime(2024, 4, 1),
                                  "tpep_dropoff_datetime": datetime(2024, 4, 1, 0, 20)},    "implausible_duration"),
    ("30_hour_trip",             {"tpep_dropoff_datetime": datetime(2024, 3, 11, 14, 0)},   "implausible_duration"),
    ("zero_distance",            {"trip_distance": 0.0},                                    "non_positive_distance"),
    ("negative_distance",        {"trip_distance": -2.0},                                   "non_positive_distance"),
    ("cross_country",            {"trip_distance": 900.0},                                  "implausible_distance"),
    ("negative_fare",            {"fare_amount": -18.0},                                    "negative_amount"),
    ("negative_total",           {"total_amount": -1.0},                                    "negative_amount"),
    ("absurd_total",             {"total_amount": 99_999.0},                                "implausible_fare"),
    ("zone_zero",                {"PULocationID": 0},                                       "unknown_zone"),
    ("zone_out_of_range",        {"DOLocationID": 900},                                     "unknown_zone"),
    ("too_many_passengers",      {"passenger_count": 77.0},                                 "implausible_passenger_count"),
]


def _write_bronze(config, year, month):
    rows = []
    for _, overrides, _ in CASES:
        row = dict(BASE)
        row.update(overrides)
        rows.append(row)
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        ("VendorID", pa.int64()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.float64()),
        ("trip_distance", pa.float64()),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("tip_amount", pa.float64()),
        ("total_amount", pa.float64()),
    ]))
    d = config.partition_path(config.BRONZE, year, month)
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, d / "trips.parquet")


@pytest.fixture
def transformed(sandbox):
    from tlc import config, silver
    _write_bronze(config, 2024, 3)
    return silver.transform(2024, 3), config


def test_no_row_is_lost(transformed):
    """Row counts reconcile: nothing is silently dropped."""
    stats, _ = transformed
    assert stats["rows_in"] == len(CASES)
    assert stats["rows_valid"] + stats["rows_rejected"] == stats["rows_in"]


def test_only_the_clean_row_survives(transformed):
    stats, _ = transformed
    expected_clean = sum(1 for _, _, reason in CASES if reason is None)
    assert stats["rows_valid"] == expected_clean


@pytest.mark.parametrize("label,overrides,reason", [c for c in CASES if c[2]])
def test_each_bad_row_gets_its_reason(transformed, label, overrides, reason):
    """Attribution matters: 'some rows were bad' is not an actionable alert."""
    stats, _ = transformed
    assert reason in stats["rejection_breakdown"], f"{label} produced no {reason}"


def test_quarantine_is_readable_and_complete(transformed):
    import duckdb
    stats, config = transformed
    con = duckdb.connect()
    n = con.execute(f"SELECT count(*) FROM read_parquet('{stats['quarantine_path']}')").fetchone()[0]
    assert n == stats["rows_rejected"]


def test_derived_columns_are_sane(transformed):
    import duckdb
    stats, _ = transformed
    con = duckdb.connect()
    row = con.execute(
        f"SELECT trip_minutes, avg_speed_mph FROM read_parquet('{stats['silver_path']}')"
    ).fetchone()
    assert row[0] == pytest.approx(20.0)
    assert row[1] == pytest.approx(3.4 / (20 / 60), rel=1e-6)


def test_schema_contract_rejects_a_renamed_column(sandbox):
    """A dropped upstream column must fail loudly, not produce null columns."""
    import duckdb
    from tlc import config, silver

    _write_bronze(config, 2024, 3)
    src = config.partition_path(config.BRONZE, 2024, 3) / "trips.parquet"
    con = duckdb.connect()
    con.execute(
        f"""COPY (SELECT * EXCLUDE (trip_distance), trip_distance AS distance_miles
            FROM read_parquet('{src}')) TO '{src}' (FORMAT parquet)"""
    )
    with pytest.raises(silver.SchemaContractError, match="missing required columns"):
        silver.transform(2024, 3)
