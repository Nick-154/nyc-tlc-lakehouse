#!/usr/bin/env bash
# Ad-hoc SQL against the gold warehouse:  ./scripts/query.sh "SELECT ..."
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python -c "
import duckdb, sys
con = duckdb.connect('data/gold/warehouse.duckdb', read_only=True)
con.sql(sys.argv[1]).show(max_rows=50)
" "$1"
