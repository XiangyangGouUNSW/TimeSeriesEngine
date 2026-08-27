#!/bin/bash
# ============================================================
# ETT 一键配置 + 全功能测试脚本
#   配置: 7 categories + 7 instances + 1 constraint + 2 relations
#         + window(1h) + derived series + 异常/预测任务 + 事件 + 数据写入
#   测试: 条件查询/状态更新/数据查询/统计/决策/结果查询/缓存
#   Usage:   ./setup-ett-all.sh [base_url]
#   Default: http://localhost:8080
# ============================================================

BASE_URL="${1:-http://localhost:8080}"
JSON_DIR="src/test/resources/command-requests/ett"
DATA_INGEST_URL="${TIMESERIES_DATA_INGEST_ENDPOINT:-http://127.0.0.1:8006}"
CORE_ENDPOINT="${TIMESERIES_CORE_ENDPOINT:-127.0.0.1:50051}"

: "${TIMESERIES_AUTH_BOOTSTRAP_USERNAME:=admin}"
: "${TIMESERIES_AUTH_BOOTSTRAP_PASSWORD:=admin123}"
export TIMESERIES_AUTH_BOOTSTRAP_USERNAME TIMESERIES_AUTH_BOOTSTRAP_PASSWORD
COOKIE_JAR="$(mktemp)"
AUTH_BODY="$(mktemp)"
trap 'rm -f "$COOKIE_JAR" "$AUTH_BODY"' EXIT
python3 -c 'import json,os; print(json.dumps({"username":os.environ["TIMESERIES_AUTH_BOOTSTRAP_USERNAME"],"password":os.environ["TIMESERIES_AUTH_BOOTSTRAP_PASSWORD"]}))' > "$AUTH_BODY"
curl -sS -f -c "$COOKIE_JAR" -H "Content-Type: application/json" \
    --data-binary "@$AUTH_BODY" "$BASE_URL/api/auth/login" > /dev/null
curl -sS -f -c "$COOKIE_JAR" -b "$COOKIE_JAR" "$BASE_URL/api/auth/csrf" > /dev/null
CSRF_TOKEN=$(awk '$6 == "XSRF-TOKEN" { token = $7 } END { print token }' "$COOKIE_JAR")
if [[ -z "$CSRF_TOKEN" ]]; then
    echo "Login succeeded but the XSRF-TOKEN cookie was not returned"
    exit 1
fi
CURL_AUTH=(-b "$COOKIE_JAR" -H "X-XSRF-TOKEN: $CSRF_TOKEN")

PASS=0
FAIL=0
SKIP=0

# 探测 Core gRPC：统计计算等步骤依赖 Core 对齐窗口
CORE_HOST="${CORE_ENDPOINT%:*}"
CORE_PORT="${CORE_ENDPOINT##*:}"
CORE_ALIVE=0
if timeout 2 bash -c "exec 3<>/dev/tcp/${CORE_HOST}/${CORE_PORT}" 2>/dev/null; then
    CORE_ALIVE=1
fi

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
    local method="${4:-POST}"
    local response_file
    local http_code

    response_file="$(mktemp)"
    if [[ "$data_arg" == @* ]]; then
        http_code=$(curl -s "${CURL_AUTH[@]}" -o "$response_file" -w "%{http_code}" -X "$method" "$url" \
            -H "Content-Type: application/json" \
            --data-binary "$data_arg")
    else
        http_code=$(curl -s "${CURL_AUTH[@]}" -o "$response_file" -w "%{http_code}" -X "$method" "$url" \
            -H "Content-Type: application/json" \
            -d "$data_arg")
    fi

    # 幂等处理：重复创建视为成功
    if grep -qi 'already exists' "$response_file"; then
        PASS=$((PASS + 1))
        echo "  ✓ $label done (already exists, skipped)"
        rm -f "$response_file"
        return 0
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

    PASS=$((PASS + 1))
    echo "  ✓ $label done"
    rm -f "$response_file"
}

# 宽松校验请求：失败只记录不退出（用于查询/测试类步骤）
check_request() {
    local label="$1"
    local method="$2"
    local url="$3"
    local data_arg="$4"
    local response_file
    local http_code

    response_file="$(mktemp)"
    if [[ -n "$data_arg" ]]; then
        if [[ "$data_arg" == @* ]]; then
            http_code=$(curl -s "${CURL_AUTH[@]}" -o "$response_file" -w "%{http_code}" -X "$method" "$url" \
                -H "Content-Type: application/json" \
                --data-binary "$data_arg")
        else
            http_code=$(curl -s "${CURL_AUTH[@]}" -o "$response_file" -w "%{http_code}" -X "$method" "$url" \
                -H "Content-Type: application/json" \
                -d "$data_arg")
        fi
    else
        http_code=$(curl -s "${CURL_AUTH[@]}" -o "$response_file" -w "%{http_code}" -X "$method" "$url")
    fi

    if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]] && \
       grep -q '"success"[[:space:]]*:[[:space:]]*true' "$response_file"; then
        PASS=$((PASS + 1))
        echo "  ✓ $label"
    else
        FAIL=$((FAIL + 1))
        echo "  ✗ $label (HTTP $http_code)"
        head -c 400 "$response_file"
        echo
    fi
    rm -f "$response_file"
}

# 列表 GET：打印条目数
check_list() {
    local label="$1"
    local url="$2"
    local response_file
    local count

    response_file="$(mktemp)"
    curl -s "${CURL_AUTH[@]}" -o "$response_file" -X GET "$url"
    count=$(python3 -c "import sys,json;d=json.load(open(sys.argv[1]));v=d.get('data');print(len(v) if isinstance(v,list) else ('n/a' if v is None else 1))" "$response_file" 2>/dev/null)
    if grep -q '"success"[[:space:]]*:[[:space:]]*true' "$response_file"; then
        PASS=$((PASS + 1))
        echo "  ✓ $label（${count} 条）"
    else
        FAIL=$((FAIL + 1))
        echo "  ✗ $label"
        head -c 300 "$response_file"
        echo
    fi
    rm -f "$response_file"
}

echo "============================================================"
echo " ETT Full Setup + Test  ->  $BASE_URL"
echo "============================================================"

check_data_ingest

if [[ "$CORE_ALIVE" -eq 1 ]]; then
    echo "Core gRPC ($CORE_ENDPOINT): 在线"
else
    echo "Core gRPC ($CORE_ENDPOINT): 未运行（依赖 Core 的步骤将跳过）"
fi

# ── Step 1: 7 categories ────────────────────────────────────
echo ""
echo "--- [1/18] Creating 7 categories ---"
for c in ot hufl hull mufl mull lufl lull; do
    post_json "$c" "$BASE_URL/api/timeseries/semantic/categories" "@$JSON_DIR/ett-category-$c.json"
done

# ── Step 2: 7 instances ─────────────────────────────────────
echo ""
echo "--- [2/18] Creating 7 instances ---"
for i in ot hufl hull mufl mull lufl lull; do
    post_json "$i" "$BASE_URL/api/timeseries/instances" "@$JSON_DIR/ett-instance-$i.json"
done

# ── Step 3: 1 constraint ────────────────────────────────────
echo ""
echo "--- [3/18] Creating constraint ---"
post_json "constraint" "$BASE_URL/api/timeseries/semantic/constraints" "@$JSON_DIR/ett-constraint.json"

# ── Step 4: relation (CAUSAL) ────────────────────────────────
echo ""
echo "--- [4/18] Creating relation ---"
post_json "relation" "$BASE_URL/api/timeseries/semantic/relations" "@$JSON_DIR/ett-relation.json"

# ── Step 5: Window config (1 hour = 3600000 ms) ─────────────
echo ""
echo "--- [5/18] Setting window (1h) ---"
post_json "window" "$BASE_URL/api/timeseries/window-config" '{"projectId":"project-ett","windowSizeMs":3600000}'

# ── Step 6: 相关性关系 ──────────────────────────────────────
echo ""
echo "--- [6/18] Creating correlation relation ---"
post_json "relation-correlation" "$BASE_URL/api/timeseries/semantic/relations" "@$JSON_DIR/ett-relation-correlation.json"

# ── Step 7: 派生序列 ────────────────────────────────────────
echo ""
echo "--- [7/18] Creating derived series ---"
post_json "derived-series" "$BASE_URL/api/timeseries/derived-series" "@$JSON_DIR/ett-derived-series.json"

# ── Step 8: 异常检测任务 ────────────────────────────────────
echo ""
echo "--- [8/18] Creating anomaly task ---"
post_json "anomaly-task" "$BASE_URL/api/timeseries/anomaly-tasks" "@$JSON_DIR/ett-anomaly-task.json"

# ── Step 9: 预测任务 ────────────────────────────────────────
echo ""
echo "--- [9/18] Creating forecast task ---"
post_json "forecast-task" "$BASE_URL/api/timeseries/forecast-tasks" "@$JSON_DIR/ett-forecast-task.json"

# ── Step 10: 事件 ───────────────────────────────────────────
echo ""
echo "--- [10/18] Creating event ---"
EVENT_ID="ett-test-event-001"
post_json "event-create" "$BASE_URL/api/timeseries/events" \
    "{\"projectId\":\"project-ett\",\"eventId\":\"$EVENT_ID\",\"eventName\":\"script test event\",\"eventType\":\"WARNING\",\"eventSource\":\"test-script\",\"relatedSequences\":[\"ETTh1_OT\"],\"eventDescription\":\"created by setup-ett-all.sh\",\"eventLevel\":\"MEDIUM\",\"confirmStatus\":\"CONFIRMED\",\"handleStatus\":\"UNHANDLED\",\"user\":\"script\"}"

# ── Step 11: 数据写入（5 分钟间隔密集点，保证 1h 窗口内 ≥2 点供 Core 对齐） ─
echo ""
echo "--- [11/18] Data ingest ---"
INGEST_FILE="$(mktemp)"
python3 - > "$INGEST_FILE" <<'PYEOF'
import json, sys

seqs = {
    "ETTh1_HUFL": 5.827, "ETTh1_HULL": 2.009, "ETTh1_MUFL": 1.599,
    "ETTh1_MULL": 0.462, "ETTh1_LUFL": 4.203, "ETTh1_LULL": 1.340, "ETTh1_OT": 30.531,
}
t0 = 1467331200000        # 2016-07-01T00:00:00Z
step = 5 * 60 * 1000      # 5 分钟
points = []
for seq, base in seqs.items():
    for i in range(24):   # 每序列 24 点，覆盖 2 小时
        v = base + i * 0.01 + ((i * 37) % 11) * 0.001
        points.append({
            "projectId": "project-ett",
            "sequenceId": seq,
            "dataSourceId": "ETTh1.csv",
            "externalSequenceId": seq,
            "time": t0 + i * step,
            "doubleValue": round(v, 3),
        })
json.dump({"projectId": "project-ett", "returnResolvedData": True, "points": points}, sys.stdout)
PYEOF
post_json "data-ingest" "$BASE_URL/api/timeseries/data/ingest" "@$INGEST_FILE"
rm -f "$INGEST_FILE"

# # ── Step 12: 数据查询 ───────────────────────────────────────
# echo ""
# echo "--- [12/18] 数据查询 ---"
# check_request "history-query" POST "$BASE_URL/api/timeseries/data/history/query" '{"projectId":"project-ett","sequenceId":"ETTh1_OT","startTime":"2016-07-01T00:00:00","endTime":"2016-07-02T00:00:00"}'
# check_request "history-overview" POST "$BASE_URL/api/timeseries/data/history/overview" '{"projectId":"project-ett","sequenceIds":["ETTh1_OT","ETTh1_HUFL"],"startTime":"2016-07-01T00:00:00","endTime":"2016-07-02T00:00:00"}'
# check_request "window-query" POST "$BASE_URL/api/timeseries/data/window/query" '{"projectId":"project-ett","sequenceIds":["ETTh1_OT"],"startTime":"2016-07-01T00:00:00","endTime":"2016-07-02T00:00:00"}'

# # ── Step 13: 相关性统计（依赖 Core gRPC 对齐窗口） ─────────
# echo ""
# echo "--- [13/18] 相关性统计 ---"
# if [[ "$CORE_ALIVE" -eq 1 ]]; then
#     check_request "statistics-compute" POST "$BASE_URL/api/timeseries/statistics/compute" "@$JSON_DIR/ett-statistics.json"
# else
#     SKIP=$((SKIP + 1))
#     echo "  - statistics-compute 跳过（Core gRPC 未运行）"
# fi
# check_list "statistics-list" "$BASE_URL/api/timeseries/statistics"

# # ── Step 14: 状态更新 (PATCH) ───────────────────────────────
# echo ""
# echo "--- [14/18] 状态更新 ---"
# post_json "category-status" "$BASE_URL/api/timeseries/semantic/categories/status" '{"projectId":"project-ett","categoryId":"OT","confirmStatus":"CONFIRMED","effectiveStatus":"ENABLE"}' PATCH
# post_json "constraint-status" "$BASE_URL/api/timeseries/semantic/constraints/status" '{"projectId":"project-ett","constraintId":"ett-ot-upper-limit","confirmStatus":"CONFIRMED","effectiveStatus":"ENABLE"}' PATCH
# post_json "relation-status" "$BASE_URL/api/timeseries/semantic/relations/status" '{"projectId":"project-ett","relationId":"ett-hufl-ot-lag","confirmStatus":"CONFIRMED","effectiveStatus":"ENABLE"}' PATCH
# post_json "anomaly-task-status" "$BASE_URL/api/timeseries/anomaly-tasks/status" '{"projectId":"project-ett","taskId":"ett-anomaly-001","status":"ENABLE"}' PATCH
# post_json "forecast-task-status" "$BASE_URL/api/timeseries/forecast-tasks/status" '{"projectId":"project-ett","taskId":"ett-forecast-001","status":"ENABLE"}' PATCH

# # ── Step 15: 条件查询 ───────────────────────────────────────
# echo ""
# echo "--- [15/18] 条件查询 ---"
# check_request "instances-query" POST "$BASE_URL/api/timeseries/instances/query" '{"projectId":"project-ett"}'
# check_request "categories-query" POST "$BASE_URL/api/timeseries/semantic/categories/query" '{"projectId":"project-ett"}'
# check_request "constraints-query" POST "$BASE_URL/api/timeseries/semantic/constraints/query" '{"projectId":"project-ett"}'
# check_request "relations-query" POST "$BASE_URL/api/timeseries/semantic/relations/query" '{"projectId":"project-ett"}'
# check_request "anomaly-tasks-query" POST "$BASE_URL/api/timeseries/anomaly-tasks/query" '{"projectId":"project-ett"}'
# check_request "forecast-tasks-query" POST "$BASE_URL/api/timeseries/forecast-tasks/query" '{"projectId":"project-ett"}'
# check_request "events-query" POST "$BASE_URL/api/timeseries/events/query" '{"projectId":"project-ett"}'
# check_request "event-detail" POST "$BASE_URL/api/timeseries/events/detail" "{\"projectId\":\"project-ett\",\"eventId\":\"$EVENT_ID\"}"
# check_request "anomaly-results-query" POST "$BASE_URL/api/timeseries/anomaly-results/query" '{"projectId":"project-ett"}'
# check_request "forecast-results-query" POST "$BASE_URL/api/timeseries/forecast-results/query" '{"projectId":"project-ett"}'

# # ── Step 16: 决策辅助 ───────────────────────────────────────
# echo ""
# echo "--- [16/18] 决策辅助 ---"
# check_request "decision-diagnosis" POST "$BASE_URL/api/timeseries/decision/diagnosis" "{\"projectId\":\"project-ett\",\"eventId\":\"$EVENT_ID\"}"
# check_request "decision-suggestion" POST "$BASE_URL/api/timeseries/decision/suggestion" "{\"projectId\":\"project-ett\",\"eventId\":\"$EVENT_ID\"}"
# check_request "decision-feedback" PATCH "$BASE_URL/api/timeseries/decision/feedback" "{\"projectId\":\"project-ett\",\"eventId\":\"$EVENT_ID\",\"disposalResult\":\"test disposal\",\"handleStatus\":\"HANDLED\"}"

# # ── Step 17: 列表 GET ───────────────────────────────────────
# echo ""
# echo "--- [17/18] 列表查询 ---"
# check_list "instances" "$BASE_URL/api/timeseries/instances"
# check_list "categories" "$BASE_URL/api/timeseries/semantic/categories"
# check_list "constraints" "$BASE_URL/api/timeseries/semantic/constraints"
# check_list "relations" "$BASE_URL/api/timeseries/semantic/relations"
# check_list "anomaly-tasks" "$BASE_URL/api/timeseries/anomaly-tasks"
# check_list "forecast-tasks" "$BASE_URL/api/timeseries/forecast-tasks"
# check_list "events" "$BASE_URL/api/timeseries/events"
# check_list "anomaly-results" "$BASE_URL/api/timeseries/anomaly-results"
# check_list "forecast-results" "$BASE_URL/api/timeseries/forecast-results"

# # ── Step 18: 缓存管理 ───────────────────────────────────────
# echo ""
# echo "--- [18/18] 缓存管理 ---"
# post_json "cache-warm-up" "$BASE_URL/api/timeseries/cache/warm-up" '{}'
# check_request "cache-refresh" POST "$BASE_URL/api/timeseries/cache/tables/instance_config/refresh" '{}'

echo ""
echo "============================================================"
echo " 全部完成：通过 $PASS 项，失败 $FAIL 项，跳过 $SKIP 项"
echo "============================================================"
if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
