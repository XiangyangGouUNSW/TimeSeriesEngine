#!/bin/bash
# ============================================================
# ETT 一键配置脚本
#   7 categories + 7 instances + 1 constraint + 1 relation + window(1h)
#   Usage:   ./setup-ett-all.sh [base_url]
#   Default: http://localhost:8080
# ============================================================

BASE_URL="${1:-http://localhost:8080}"
JSON_DIR="src/test/resources/command-requests/ett"
DATA_INGEST_URL="${TIMESERIES_DATA_INGEST_ENDPOINT:-http://127.0.0.1:8006}"

check_data_ingest() {
    local response_file
    local http_code

    response_file="$(mktemp)"
    http_code=$(curl -s -o "$response_file" -w "%{http_code}" -m 5 "$DATA_INGEST_URL/health")
    if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        echo "DataIngest health check failed: HTTP $http_code ($DATA_INGEST_URL/health)"
        cat "$response_file"
        echo
        rm -f "$response_file"
        exit 1
    fi
    if ! grep -q '"gstore_connected"[[:space:]]*:[[:space:]]*true' "$response_file"; then
        echo "DataIngest is running, but gStore is not connected:"
        cat "$response_file"
        echo
        rm -f "$response_file"
        exit 1
    fi
    rm -f "$response_file"
}

post_json() {
    local label="$1"
    local url="$2"
    local data_arg="$3"
    local response_file
    local http_code

    response_file="$(mktemp)"
    if [[ "$data_arg" == @* ]]; then
        http_code=$(curl -s -o "$response_file" -w "%{http_code}" -X POST "$url" \
            -H "Content-Type: application/json" \
            --data-binary "$data_arg")
    else
        http_code=$(curl -s -o "$response_file" -w "%{http_code}" -X POST "$url" \
            -H "Content-Type: application/json" \
            -d "$data_arg")
    fi

    if [[ "$http_code" -lt 200 || "$http_code" -ge 300 ]]; then
        echo "  $label failed: HTTP $http_code"
        cat "$response_file"
        echo
        rm -f "$response_file"
        exit 1
    fi

    if grep -q '"success"[[:space:]]*:[[:space:]]*false' "$response_file"; then
        echo "  $label failed: response success=false"
        cat "$response_file"
        echo
        rm -f "$response_file"
        exit 1
    fi

    echo "  $label done"
    rm -f "$response_file"
}

echo "============================================================"
echo " ETT Full Setup  ->  $BASE_URL"
echo "============================================================"

check_data_ingest

# ── Step 1: 7 categories ────────────────────────────────────
echo ""
echo "--- [1/5] Creating 7 categories ---"
for c in ot hufl hull mufl mull lufl lull; do
    post_json "$c" "$BASE_URL/api/timeseries/semantic/categories" "@$JSON_DIR/ett-category-$c.json"
done

# ── Step 2: 7 instances ─────────────────────────────────────
echo ""
echo "--- [2/5] Creating 7 instances ---"
for i in ot hufl hull mufl mull lufl lull; do
    post_json "$i" "$BASE_URL/api/timeseries/instances" "@$JSON_DIR/ett-instance-$i.json"
done

# ── Step 3: 1 constraint ────────────────────────────────────
echo ""
echo "--- [3/5] Creating constraint ---"
post_json "constraint" "$BASE_URL/api/timeseries/semantic/constraints" "@$JSON_DIR/ett-constraint.json"

# ── Step 4: 1 relation ──────────────────────────────────────
echo ""
echo "--- [4/5] Creating relation ---"
post_json "relation" "$BASE_URL/api/timeseries/semantic/relations" "@$JSON_DIR/ett-relation.json"

# ── Step 5: Window config (1 hour = 3600000 ms) ─────────────
echo ""
echo "--- [5/5] Setting window (1h) ---"
post_json "window" "$BASE_URL/api/timeseries/window-config" '{"windowSizeMs":3600000}'

echo ""
echo "============================================================"
echo " ETT setup complete!"
echo "============================================================"
