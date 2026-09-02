"""Structural checks that catch a broken DAG before the scheduler does."""
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dagbag(monkeypatch_session=None):
    import os
    os.environ.setdefault("AIRFLOW_HOME", str(ROOT))
    from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=str(ROOT / "dags"))


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors


def test_dag_present(dagbag):
    assert "tlc_lakehouse" in dagbag.dags


def test_no_cycles(dagbag):
    dagbag.dags["tlc_lakehouse"].check_cycle()


def test_every_task_retries(dagbag):
    """An unretried task in a network-bound pipeline is a pager at 3am."""
    for task in dagbag.dags["tlc_lakehouse"].tasks:
        assert task.retries >= 1, f"{task.task_id} has no retries"


def test_uses_data_interval_timetable(dagbag):
    """Partitioning depends on a real interval, not a zero-length trigger."""
    from airflow.timetables.interval import CronDataIntervalTimetable
    assert isinstance(dagbag.dags["tlc_lakehouse"].timetable, CronDataIntervalTimetable)


def test_warehouse_writes_are_serialised(dagbag):
    """DuckDB takes one writer; concurrent months must not both load gold."""
    dag = dagbag.dags["tlc_lakehouse"]
    for task_id in ("load_gold", "update_zone_dimension", "record_run"):
        assert dag.get_task(task_id).max_active_tis_per_dag == 1, task_id


def test_gold_waits_for_the_quality_gate(dagbag):
    """The gate is only a gate if nothing can route around it."""
    dag = dagbag.dags["tlc_lakehouse"]
    assert "quality_gate" in dag.get_task("load_gold").upstream_task_ids
