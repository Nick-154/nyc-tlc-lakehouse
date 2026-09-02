"""Quality gate: the last point at which a bad partition can be stopped.

Checks run against silver, before anything reaches gold. A hard failure raises
and the partition never loads; a soft failure is recorded and the run proceeds.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb

from . import config

log = logging.getLogger(__name__)

KEY_COLUMNS = ("pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount")


@dataclass
class Check:
    name: str
    passed: bool
    severity: str            # "hard" | "soft"
    observed: float
    threshold: float
    detail: str = ""


@dataclass
class GateResult:
    checks: list[Check] = field(default_factory=list)

    @property
    def hard_failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "hard"]

    @property
    def soft_failures(self) -> list[Check]:
        return [c for c in self.checks if not c.passed and c.severity == "soft"]

    @property
    def passed(self) -> bool:
        return not self.hard_failures

    def summary(self) -> dict:
        return {
            "passed": self.passed,
            "n_checks": len(self.checks),
            "hard_failures": [c.name for c in self.hard_failures],
            "soft_failures": [c.name for c in self.soft_failures],
            "checks": [c.__dict__ for c in self.checks],
        }


class QualityGateFailed(Exception):
    pass


def run_gate(year: int, month: int, silver_stats: dict) -> dict:
    src = config.partition_path(config.SILVER, year, month) / "trips.parquet"
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE TEMP VIEW s AS SELECT * FROM read_parquet('{src}')")

    rows = con.execute("SELECT count(*) FROM s").fetchone()[0]
    res = GateResult()

    res.checks.append(Check(
        "row_volume", rows >= config.MIN_ROWS_PER_MONTH, "hard",
        rows, config.MIN_ROWS_PER_MONTH,
        "a real month of yellow-cab trips is millions of rows; far fewer means a partial source file",
    ))

    rate = silver_stats["reject_rate"]
    res.checks.append(Check(
        "reject_rate_hard", rate <= config.MAX_REJECT_RATE, "hard",
        round(rate, 5), config.MAX_REJECT_RATE,
        f"breakdown: {silver_stats.get('rejection_breakdown')}",
    ))
    res.checks.append(Check(
        "reject_rate_soft", rate <= config.WARN_REJECT_RATE, "soft",
        round(rate, 5), config.WARN_REJECT_RATE,
    ))

    for col in KEY_COLUMNS:
        nulls = con.execute(f"SELECT count(*) FILTER (WHERE {col} IS NULL) FROM s").fetchone()[0]
        nr = nulls / rows if rows else 0.0
        res.checks.append(Check(
            f"null_rate.{col}", nr <= config.MAX_NULL_RATE_KEY_COLS, "hard",
            round(nr, 6), config.MAX_NULL_RATE_KEY_COLS,
        ))

    # Every surviving row must belong to the partition it is stored under,
    # otherwise a backfill of one month can corrupt another month's totals.
    stray = con.execute(
        f"SELECT count(*) FROM s WHERE year(pickup_ts) <> {year} OR month(pickup_ts) <> {month}"
    ).fetchone()[0]
    res.checks.append(Check(
        "partition_alignment", stray == 0, "hard", stray, 0,
        "rows whose pickup date falls outside the partition they are stored in",
    ))

    dupes = con.execute(
        """
        SELECT count(*) FROM (
          SELECT 1 FROM s
          GROUP BY vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, total_amount
          HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    dupe_rate = dupes / rows if rows else 0.0
    res.checks.append(Check(
        "duplicate_trip_signatures", dupe_rate <= 0.02, "soft", round(dupe_rate, 5), 0.02,
        "identical vendor/time/zone/amount tuples; the source has no trip id so these are only probable duplicates",
    ))

    neg = con.execute("SELECT count(*) FROM s WHERE total_amount < 0 OR trip_miles <= 0").fetchone()[0]
    res.checks.append(Check(
        "silver_invariants", neg == 0, "hard", neg, 0,
        "rows that should have been quarantined but reached silver",
    ))

    con.close()
    out = res.summary()

    for c in res.checks:
        log.log(logging.INFO if c.passed else logging.WARNING,
                "check %-32s %-4s observed=%s threshold=%s %s",
                c.name, "PASS" if c.passed else "FAIL", c.observed, c.threshold,
                "" if c.passed else c.detail)

    if res.hard_failures:
        raise QualityGateFailed(
            f"{year:04d}-{month:02d} failed {len(res.hard_failures)} hard check(s): "
            + ", ".join(f"{c.name}(observed={c.observed}, limit={c.threshold})" for c in res.hard_failures)
        )
    return out
