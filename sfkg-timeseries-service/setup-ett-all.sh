#!/bin/bash
# ============================================================
# ETT 一键配置脚本
#   7 categories + 7 instances + 1 constraint + 1 relation + window(1h)
#   Usage:   ./setup-ett-all.sh [base_url]
#   Default: http://localhost:8080
# ============================================================

BASE_URL="${1:-http://localhost:8080}"
JSON_DIR="src/test/resources/command-requests/ett"

echo "============================================================"
echo " ETT Full Setup  ->  $BASE_URL"
echo "============================================================"

# ── Step 1: 7 categories ────────────────────────────────────
echo ""
echo "--- [1/5] Creating 7 categories ---"
for c in ot hufl hull mufl mull lufl lull; do
    curl -s -X POST "$BASE_URL/api/timeseries/semantic/categories" \
        -H "Content-Type: application/json" \
        --data-binary "@$JSON_DIR/ett-category-$c.json"
    echo "  $c done"
done

# ── Step 2: 7 instances ─────────────────────────────────────
echo ""
echo "--- [2/5] Creating 7 instances ---"
for i in ot hufl hull mufl mull lufl lull; do
    curl -s -X POST "$BASE_URL/api/timeseries/instances" \
        -H "Content-Type: application/json" \
        --data-binary "@$JSON_DIR/ett-instance-$i.json"
    echo "  $i done"
done

# ── Step 3: 1 constraint ────────────────────────────────────
echo ""
echo "--- [3/5] Creating constraint ---"
curl -s -X POST "$BASE_URL/api/timeseries/semantic/constraints" \
    -H "Content-Type: application/json" \
    --data-binary "@$JSON_DIR/ett-constraint.json"
echo "  constraint done"

# ── Step 4: 1 relation ──────────────────────────────────────
echo ""
echo "--- [4/5] Creating relation ---"
curl -s -X POST "$BASE_URL/api/timeseries/semantic/relations" \
    -H "Content-Type: application/json" \
    --data-binary "@$JSON_DIR/ett-relation.json"
echo "  relation done"

# ── Step 5: Window config (1 hour = 3600000 ms) ─────────────
echo ""
echo "--- [5/5] Setting window (1h) ---"
curl -s -X POST "$BASE_URL/api/timeseries/window-config" \
    -H "Content-Type: application/json" \
    -d '{"windowSizeMs":3600000}'
echo "  window done"

echo ""
echo "============================================================"
echo " ETT setup complete!"
echo "============================================================"
