#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

RUNTIME_LIB_DIR="${PROJECT_DIR}/.runtime/toolchain/usr/lib/x86_64-linux-gnu"
TAOS_LIB_DIR="${PROJECT_DIR}/.runtime/tdengine/usr/local/taos/driver"
export LD_LIBRARY_PATH="${RUNTIME_LIB_DIR}:${TAOS_LIB_DIR}:/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

exec "${PROJECT_DIR}/build-taos/grpc_ordered_load_test" "$@"
