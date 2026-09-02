#!/usr/bin/env bash
# Backfill a range of months, inclusive.
#   ./scripts/backfill.sh 2024-01 2024-06
#
# Note on the date you pass to `airflow dags test`: a manual run infers the
# enclosing *completed* interval, so testing partition 2024-01 means passing
# 2024-02-01. This script does that conversion so you can think in partitions.
set -euo pipefail
cd "$(dirname "$0")/.."
export AIRFLOW_HOME="$PWD"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

from="${1:?usage: backfill.sh YYYY-MM YYYY-MM}"; to="${2:?usage: backfill.sh YYYY-MM YYYY-MM}"
y=${from%-*}; m=${from#*-}; ey=${to%-*}; em=${to#*-}

while [ $((10#$y * 12 + 10#$m)) -le $((10#$ey * 12 + 10#$em)) ]; do
  ny=$y; nm=$((10#$m + 1)); [ $nm -gt 12 ] && { nm=1; ny=$((10#$y + 1)); }
  printf '\n=== partition %04d-%02d ===\n' "$y" "$m"
  .venv/bin/airflow dags test tlc_lakehouse "$(printf '%04d-%02d-01' "$ny" "$nm")" 2>&1 \
    | grep -E 'silver [0-9]{4}|gold [0-9]{4}|FAIL|state=success' | sed -E 's/.*\] //' | tail -5
  y=$ny; m=$nm
done
