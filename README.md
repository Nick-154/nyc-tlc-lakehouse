# NYC TLC Lakehouse

An incremental data pipeline over NYC yellow-taxi trip records. Airflow orchestrates
it, DuckDB is the warehouse, and everything lands as partitioned Parquet. Runs
entirely on a laptop with no Docker and no cloud account.

Four months loaded so far: **12.6M trips, $349M in fares, 448,816 rows quarantined.**

| partition | rows in | kept | quarantined | reject rate | gate |
|---|---|---|---|---|---|
| 2024-01 | 2,964,624 | 2,868,374 | 96,250 | 3.25% | pass |
| 2024-02 | 3,007,526 | 2,900,195 | 107,331 | 3.57% | pass |
| 2024-03 | 3,582,628 | 3,438,749 | 143,879 | 4.02% | pass |
| 2024-04 | 3,514,289 | 3,412,933 | 101,356 | 2.88% | pass |

A month takes about 4 seconds end to end on an M-series laptop, most of it download.

## What this is actually about

Loading data is the easy part. The pipeline is built around the three things that
break real pipelines, and each one is enforced in code rather than described in a doc.

**Re-running a month must not change the answer.** Every write is scoped to the
partition it belongs to. Bronze downloads to a temp file and `os.replace`s it into
position, which is atomic, so a killed process cannot leave a truncated Parquet for
the next task to read. Silver writes both its outputs the same way. Gold does
delete-then-insert inside a transaction, keyed on `(year, month)`, so reloading March
replaces March and touches nothing else. Running the same partition four times gives
byte-identical totals, which the test suite asserts rather than assumes.

**Bad data must be visible, not silently dropped.** Every source row ends in exactly
one of two places, silver or quarantine, so counts always reconcile. Quarantined rows
keep their original values plus the specific rule they broke, so "3.2% rejected"
decomposes into 59,594 zero-distance trips and 34,140 negative fares instead of being
one unactionable number. Across four months the source contained 65 trips whose pickup
date falls outside the month the file claims to cover, including records dated 2009.

**History must survive the source changing under you.** Zone attributes are stored as
a Type 2 dimension with `[valid_from, valid_to]` ranges, so a report for March 2024 run
today gives the same answer it would have given in March. Facts join to the dimension
on the pickup date falling inside the validity range, not on the current row.

## Layout

```
bronze/year=2024/month=01/trips.parquet   raw source, untouched, checksummed
silver/year=2024/month=01/trips.parquet   typed, cleaned, enriched
quarantine/year=2024/month=01/rejects.parquet   rejected rows + reason
gold/warehouse.duckdb                     fct_trips_daily, fct_trips_hourly,
                                          dim_zone (SCD2), pipeline_runs
```

`pipeline_runs` is the audit trail: row counts, reject rate, gate result, and the
source SHA-256 for every partition ever loaded.

## The DAG

```
wait_for_source -> ingest_bronze -> build_silver -> quality_gate -> load_gold -> record_run
                                    update_zone_dimension --------------------->
```

A few decisions worth explaining:

**The partition comes from the run's data interval, never from the clock.** A run for
2024-03 processes 2024-03 whether it fires on schedule or as part of a backfill two
years later. Backfill and live execution are the same code path, so backfill is not a
separate script that drifts out of sync.

**The schedule is an explicit `CronDataIntervalTimetable`, not `@monthly`.** Airflow 3
resolves cron strings to a trigger timetable whose data interval is zero-length, which
would fire January's run on January 1st, before the month has happened. The explicit
timetable gives each run the interval `[month start, next month)` and fires it once the
month is complete.

**`wait_for_source` is a sensor in reschedule mode.** TLC publishes roughly two months
in arrears. Reschedule frees the worker slot between pokes, which matters when several
months are queued behind a file that has not landed.

**Warehouse writes are serialised with `max_active_tis_per_dag=1`** while download and
transform still run three months wide. DuckDB accepts a single writer. A served
warehouse would not need this, and the constraint is on the tasks rather than the DAG
so the expensive stages stay parallel.

**The quality gate separates hard from soft.** A hard breach raises and the partition
never reaches gold. A soft breach is recorded and the run continues. Reject rate has
both: above 5% is a broken source and stops the load, above 1% is worth a look. All
four months tripped the soft threshold, which is correct, that is what the real data
looks like.

## Running it

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python "apache-airflow==3.3.1" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt
uv pip install --python .venv/bin/python duckdb pandas pyarrow pytest

source env.sh
.venv/bin/airflow db migrate
.venv/bin/airflow dags reserialize

./scripts/backfill.sh 2024-01 2024-04      # load four months
./scripts/query.sh "SELECT * FROM pipeline_runs"
.venv/bin/python -m pytest tests -q        # 33 tests
```

For the Airflow UI: `.venv/bin/airflow standalone`, then unpause `tlc_lakehouse`.

One sharp edge worth knowing: `airflow dags test` creates a *manual* run, and manual
runs infer the enclosing completed interval. Testing partition 2024-01 means passing
`2024-02-01`. `scripts/backfill.sh` does that conversion so you can think in partitions.

## Tests

33 tests, no network required, none of them touch the real warehouse.

- **DAG integrity**: imports cleanly, no cycles, every task has retries, the timetable
  is the interval kind, warehouse writers are serialised, and nothing can route around
  the quality gate.
- **Silver rules**: a synthetic Parquet with sixteen hand-built rows, one per failure
  mode, asserting each lands in quarantine under the right reason and that counts
  reconcile. Includes a dropped-column case, which must fail loudly rather than produce
  a table of nulls.
- **SCD2**: two snapshots where one zone is reclassified, one is added and one
  disappears. Asserts a second version opens, ranges abut without overlapping, a
  point-in-time query returns the historical value, retired zones stay queryable, and
  re-applying the same snapshot is a no-op.

## Known limits

DuckDB is embedded, so one writer at a time; Postgres would lift that. The zone lookup
that TLC publishes is a single current snapshot, so the SCD2 loader is exercised against
real data but only observes change in the tests. Trip records have no natural key, so
duplicate detection is a soft check on a `(vendor, timestamps, zones, amount)` signature
and cannot be authoritative.
