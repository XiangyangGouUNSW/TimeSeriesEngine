#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
default_taos_root="${repo_root}/.runtime/tdengine/usr/local/taos"
default_taos_config_dir="${repo_root}/.runtime/tdengine-runtime/cfg"

taos_root="${SFKG_TAOS_ROOT:-${default_taos_root}}"
taos_bin="${SFKG_TAOS_BIN:-${taos_root}/bin/taos}"
if [[ -n "${SFKG_TAOS_CONFIG_DIR:-}" ]]; then
    taos_config_dir="${SFKG_TAOS_CONFIG_DIR}"
elif [[ -d "${default_taos_config_dir}" ]]; then
    # The repository's unprivileged TDengine instance keeps its live client
    # configuration outside the unpacked installation tree.
    taos_config_dir="${default_taos_config_dir}"
else
    taos_config_dir="${taos_root}/cfg"
fi
if [[ -n "${SFKG_TAOS_LIB_DIR:-}" ]]; then
    taos_lib_dir="${SFKG_TAOS_LIB_DIR}"
elif [[ -d "${taos_root}/driver" ]]; then
    taos_lib_dir="${taos_root}/driver"
else
    taos_lib_dir="${taos_root}/lib"
fi
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
  SFKG_TAOS_LIB_DIR      TDengine client library directory
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

export LD_LIBRARY_PATH="${taos_lib_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

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
