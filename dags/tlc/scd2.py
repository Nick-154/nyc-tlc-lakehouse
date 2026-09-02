"""Type 2 slowly-changing dimension for taxi zones.

Zone attributes change between TLC releases: zones get renamed and boroughs get
reclassified. Overwriting them rewrites history, so a report for March 2024 run
today would silently disagree with the same report run last year. This keeps one
row per version of a zone, bounded by [valid_from, valid_to].

The merge is idempotent. Applying the same snapshot at the same effective date
twice produces no second version, because comparison is on a content hash rather
than on "did we see this row again".
"""
from __future__ import annotations

import logging
import os
import urllib.request
from datetime import date
from pathlib import Path

import duckdb

from . import config
from .gold import warehouse

log = logging.getLogger(__name__)

OPEN_ENDED = date(9999, 12, 31)

DDL = """
CREATE SEQUENCE IF NOT EXISTS dim_zone_key_seq START 1;
CREATE TABLE IF NOT EXISTS dim_zone (
    zone_key      BIGINT   NOT NULL,
    location_id   SMALLINT NOT NULL,
    borough       VARCHAR,
    zone          VARCHAR,
    service_zone  VARCHAR,
    row_hash      VARCHAR  NOT NULL,
    valid_from    DATE     NOT NULL,
    valid_to      DATE     NOT NULL,
    is_current    BOOLEAN  NOT NULL
);
"""

HASH_EXPR = "md5(coalesce(borough,'~') || '|' || coalesce(zone,'~') || '|' || coalesce(service_zone,'~'))"


def fetch_snapshot(effective: date) -> Path:
    """Land the zone lookup for a given effective date, atomically."""
    dest_dir = config.BRONZE / "zones" / f"snapshot_date={effective.isoformat()}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "taxi_zone_lookup.csv"
    if dest.exists():
        return dest
    tmp = dest_dir / ".lookup.tmp"
    try:
        with urllib.request.urlopen(config.ZONE_LOOKUP_URL, timeout=120) as r, tmp.open("wb") as fh:
            fh.write(r.read())
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)
    return dest


def merge(snapshot_csv: str | Path, effective: date, con: duckdb.DuckDBPyConnection | None = None) -> dict:
    """Merge one snapshot into dim_zone, versioning whatever changed."""
    owns = con is None
    ctx = warehouse() if owns else None
    con = ctx.__enter__() if owns else con
    try:
        con.execute(DDL)
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE snap AS
            SELECT CAST(LocationID AS SMALLINT) AS location_id,
                   Borough AS borough, Zone AS zone, service_zone,
                   {HASH_EXPR} AS row_hash
            FROM read_csv('{snapshot_csv}', header=true, AUTO_DETECT=true)
            WHERE LocationID IS NOT NULL
            """
        )

        # Refuse to apply a snapshot older than history already recorded: it
        # would open a version that overlaps a later one.
        latest = con.execute("SELECT max(valid_from) FROM dim_zone WHERE is_current").fetchone()[0]
        if latest is not None and effective < latest:
            raise ValueError(
                f"snapshot effective {effective} predates current dimension state {latest}; "
                "out-of-order SCD2 loads would create overlapping validity ranges"
            )

        con.execute("BEGIN TRANSACTION")
        try:
            changed = con.execute(
                """
                SELECT count(*) FROM dim_zone d JOIN snap s USING (location_id)
                WHERE d.is_current AND d.row_hash <> s.row_hash
                """
            ).fetchone()[0]
            retired = con.execute(
                """
                SELECT count(*) FROM dim_zone d
                WHERE d.is_current AND d.location_id NOT IN (SELECT location_id FROM snap)
                """
            ).fetchone()[0]
            new = con.execute(
                """
                SELECT count(*) FROM snap s
                WHERE s.location_id NOT IN (SELECT location_id FROM dim_zone WHERE is_current)
                """
            ).fetchone()[0]

            # 1. Close every current row that either changed or disappeared.
            con.execute(
                """
                UPDATE dim_zone SET valid_to = ? - INTERVAL 1 DAY, is_current = false
                WHERE is_current AND (
                    location_id IN (SELECT s.location_id FROM snap s
                                    WHERE s.row_hash <> dim_zone.row_hash
                                      AND s.location_id = dim_zone.location_id)
                    OR location_id NOT IN (SELECT location_id FROM snap)
                )
                """,
                [effective],
            )

            # 2. Open a version for anything new or changed. Unchanged keys are
            #    untouched, which is what makes a repeat run a no-op.
            con.execute(
                """
                INSERT INTO dim_zone
                SELECT nextval('dim_zone_key_seq'), s.location_id, s.borough, s.zone,
                       s.service_zone, s.row_hash, ?, ?, true
                FROM snap s
                WHERE s.location_id NOT IN (SELECT location_id FROM dim_zone WHERE is_current)
                """,
                [effective, OPEN_ENDED],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        total, current = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE is_current) FROM dim_zone"
        ).fetchone()
    finally:
        if owns:
            ctx.__exit__(None, None, None)

    stats = {
        "effective_date": effective.isoformat(),
        "new_zones": new, "changed_zones": changed, "retired_zones": retired,
        "rows_total": total, "rows_current": current,
    }
    log.info("dim_zone @%s: %d new, %d changed, %d retired -> %d rows (%d current)",
             effective, new, changed, retired, total, current)
    return stats


def validate(con: duckdb.DuckDBPyConnection | None = None) -> dict:
    """Assert the dimension's core invariants. Cheap, and catches merge bugs."""
    owns = con is None
    ctx = warehouse(read_only=True) if owns else None
    con = ctx.__enter__() if owns else con
    try:
        multi_current = con.execute(
            "SELECT count(*) FROM (SELECT 1 FROM dim_zone WHERE is_current GROUP BY location_id HAVING count(*) > 1)"
        ).fetchone()[0]
        overlaps = con.execute(
            """
            SELECT count(*) FROM dim_zone a JOIN dim_zone b
              ON a.location_id = b.location_id AND a.zone_key < b.zone_key
            WHERE a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to
            """
        ).fetchone()[0]
        backwards = con.execute("SELECT count(*) FROM dim_zone WHERE valid_to < valid_from").fetchone()[0]
    finally:
        if owns:
            ctx.__exit__(None, None, None)

    problems = {"multiple_current_versions": multi_current,
                "overlapping_validity_ranges": overlaps,
                "valid_to_before_valid_from": backwards}
    failed = {k: v for k, v in problems.items() if v}
    if failed:
        raise AssertionError(f"dim_zone violates SCD2 invariants: {failed}")
    log.info("dim_zone invariants hold: %s", problems)
    return problems
