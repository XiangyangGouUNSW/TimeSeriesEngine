#!/usr/bin/env bash

set -euo pipefail

taos_root="${SFKG_TAOS_ROOT:-/home/yumiduo/sfkg/tdengine}"
taos_bin="${SFKG_TAOS_BIN:-${taos_root}/bin/taos}"
taos_config_dir="${SFKG_TAOS_CONFIG_DIR:-${taos_root}/cfg}"
database="${SFKG_TAOS_DB:-sfkg_timeseries}"
raw_stable="${SFKG_TAOS_RAW_STABLE:-raw_timeseries_data}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'USAGE'
Usage: clear_sfkg_timeseries_db.sh [--yes]

Deletes all rows from sfkg_timeseries.raw_timeseries_data while preserving
the database, supertable and child-table schema. By default, the script asks
for an explicit confirmation. Use --yes only for an intentional test reset.

Environment overrides:
  SFKG_TAOS_ROOT         TDengine installation root
  SFKG_TAOS_BIN          taos CLI path
  SFKG_TAOS_CONFIG_DIR   TDengine config directory
USAGE
    exit 0
fi

if [[ "${database}" != "sfkg_timeseries" ||
      "${raw_stable}" != "raw_timeseries_data" ]]; then
    echo "refusing unexpected target: ${database}.${raw_stable}" >&2
    echo "this test-reset script only permits sfkg_timeseries.raw_timeseries_data" >&2
    exit 2
fi

if [[ ! -x "${taos_bin}" ]]; then
    echo "taos CLI not found or not executable: ${taos_bin}" >&2
    exit 1
fi
if [[ ! -d "${taos_config_dir}" ]]; then
    echo "TDengine config directory not found: ${taos_config_dir}" >&2
    exit 1
fi

export LD_LIBRARY_PATH="${taos_root}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

run_sql() {
    "${taos_bin}" -c "${taos_config_dir}" -s "$1"
}

if [[ "${1:-}" != "--yes" ]]; then
    echo "This will permanently delete all rows from:" >&2
    echo "  ${database}.${raw_stable}" >&2
    read -r -p "Type sfkg_timeseries to continue: " confirmation
    if [[ "${confirmation}" != "sfkg_timeseries" ]]; then
        echo "aborted" >&2
        exit 1
    fi
fi

echo "Before cleanup:"
run_sql "SELECT COUNT(*) AS total_rows FROM ${database}.${raw_stable};"

echo "Deleting rows..."
run_sql "DELETE FROM ${database}.${raw_stable};"

echo "After cleanup:"
run_sql "SELECT COUNT(*) AS total_rows FROM ${database}.${raw_stable};"
