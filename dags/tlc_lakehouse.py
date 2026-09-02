"""NYC TLC yellow-taxi lakehouse: bronze -> silver -> gold, one month per run.

Design notes that matter more than the code:

* The partition key comes from the run's data interval, never from wall clock
  time. A run for 2024-03 processes 2024-03 whether it fires on schedule today
  or as part of a backfill next year, so backfills and live runs are the same
  code path.
* Every write is replace-scoped to its own partition, so re-running a month is
  safe and produces identical output rather than duplicated rows.
* Writes to the embedded warehouse are serialised with max_active_tis_per_dag
  while the expensive download and transform stages still run in parallel
  across months. DuckDB takes a single writer; a served warehouse would not
  need this.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable

from tlc import bronze, config, gold, quality, scd2

log = logging.getLogger(__name__)


def _partition() -> tuple[int, int]:
    """Year and month this run owns, taken from its data interval."""
    ctx = get_current_context()
    start = ctx["data_interval_start"]
    return start.year, start.month


@dag(
    dag_id="tlc_lakehouse",
    description="Incremental NYC taxi lakehouse with quality gates and a Type 2 zone dimension",
    # Explicit data-interval timetable, not "@monthly". Airflow 3 resolves cron
    # strings to a trigger timetable whose data interval is zero-length, which
    # would fire January's run on January 1st, before the month has happened.
    # This variant gives each run the closed interval [month start, next month)
    # and fires it once that month is complete.
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=datetime(2024, 1, 1),
    catchup=False,               # backfills are explicit: airflow backfill create
    max_active_runs=3,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["lakehouse", "duckdb", "incremental", "scd2"],
    doc_md=__doc__,
)
def tlc_lakehouse():

    @task.sensor(poke_interval=3600, timeout=7 * 24 * 3600, mode="reschedule")
    def wait_for_source():
        """TLC publishes about two months in arrears, so wait rather than fail.

        Reschedule mode frees the worker slot between pokes, which matters when
        several months are queued behind a source that has not landed yet.
        """
        year, month = _partition()
        available, size = bronze.source_exists(year, month)
        log.info("source %04d-%02d available=%s size=%s", year, month, available, size)
        return available

    @task
    def ingest_bronze() -> dict:
        year, month = _partition()
        return bronze.ingest(year, month)

    @task
    def build_silver(bronze_meta: dict) -> dict:
        return silver_transform(bronze_meta["year"], bronze_meta["month"])

    @task
    def quality_gate(silver_stats: dict) -> dict:
        """Hard failures raise here, so a bad month never reaches gold."""
        return quality.run_gate(silver_stats["year"], silver_stats["month"], silver_stats)

    @task(max_active_tis_per_dag=1)
    def load_gold(silver_stats: dict, gate: dict) -> dict:
        return gold.load_partition(silver_stats["year"], silver_stats["month"])

    @task(max_active_tis_per_dag=1)
    def update_zone_dimension() -> dict:
        """Version the zone attributes as of this partition's start date.

        Most months this is a no-op because the lookup did not change, which is
        the correct and common outcome for a Type 2 merge.
        """
        ctx = get_current_context()
        effective = ctx["data_interval_start"].date()
        snapshot = scd2.fetch_snapshot(effective)
        stats = scd2.merge(snapshot, effective)
        scd2.validate()
        return stats

    @task(max_active_tis_per_dag=1)
    def record_run(bronze_meta: dict, silver_stats: dict, gate: dict,
                   gold_stats: dict, dim_stats: dict) -> dict:
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id
        gold.record_run(run_id, silver_stats["year"], silver_stats["month"],
                        silver_stats, gate, bronze_meta)
        summary = {
            "partition": f"{silver_stats['year']:04d}-{silver_stats['month']:02d}",
            "rows_in": silver_stats["rows_in"],
            "rows_valid": silver_stats["rows_valid"],
            "rows_rejected": silver_stats["rows_rejected"],
            "reject_rate_pct": round(silver_stats["reject_rate"] * 100, 3),
            "rejection_breakdown": silver_stats["rejection_breakdown"],
            "gate_soft_failures": gate["soft_failures"],
            "gold_trips": gold_stats["trips"],
            "gold_revenue": round(gold_stats["revenue"], 2),
            "dim_zone": dim_stats,
        }
        log.info("run summary: %s", summary)
        return summary

    source_ready = wait_for_source()
    bronze_meta = ingest_bronze()
    source_ready >> bronze_meta

    silver_stats = build_silver(bronze_meta)
    gate = quality_gate(silver_stats)
    gold_stats = load_gold(silver_stats, gate)
    dim_stats = update_zone_dimension()

    record_run(bronze_meta, silver_stats, gate, gold_stats, dim_stats)


def silver_transform(year: int, month: int) -> dict:
    """Indirection so tests can patch the heavy transform without an Airflow run."""
    from tlc import silver
    return silver.transform(year, month)


tlc_lakehouse()
