#!/bin/bash
set -e

BASE_URL="http://localhost:8080"
REQ_DIR="$(dirname "$0")/src/test/resources/command-requests"

echo "[1] timeseries instance create"
curl -X POST "$BASE_URL/api/timeseries/instances" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/instance-save.json"
echo -e "\n"

echo "[2] timeseries instance update"
curl -X PUT "$BASE_URL/api/timeseries/instances" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/instance-save.json"
echo -e "\n"

echo "[3] timeseries instance query"
curl -X POST "$BASE_URL/api/timeseries/instances/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/instance-query.json"
echo -e "\n"

echo "[4] history data query"
curl -X POST "$BASE_URL/api/timeseries/data/history/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/history-query.json"
echo -e "\n"

echo "[5] semantic category query"
curl -X POST "$BASE_URL/api/timeseries/semantic/categories/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/category-query.json"
echo -e "\n"

echo "[6] semantic category create"
curl -X POST "$BASE_URL/api/timeseries/semantic/categories" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/category-save.json"
echo -e "\n"

echo "[7] semantic category update"
curl -X PUT "$BASE_URL/api/timeseries/semantic/categories" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/category-save.json"
echo -e "\n"

echo "[8] semantic category status update"
curl -X PATCH "$BASE_URL/api/timeseries/semantic/categories/status" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/category-status.json"
echo -e "\n"

echo "[9] semantic constraint query"
curl -X POST "$BASE_URL/api/timeseries/semantic/constraints/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/constraint-query.json"
echo -e "\n"

echo "[10] semantic constraint create"
curl -X POST "$BASE_URL/api/timeseries/semantic/constraints" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/constraint-save.json"
echo -e "\n"

echo "[11] semantic constraint update"
curl -X PUT "$BASE_URL/api/timeseries/semantic/constraints" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/constraint-save.json"
echo -e "\n"

echo "[12] semantic constraint status update"
curl -X PATCH "$BASE_URL/api/timeseries/semantic/constraints/status" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/constraint-status.json"
echo -e "\n"

echo "[13] semantic relation query"
curl -X POST "$BASE_URL/api/timeseries/semantic/relations/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/relation-query.json"
echo -e "\n"

echo "[14] semantic relation create"
curl -X POST "$BASE_URL/api/timeseries/semantic/relations" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/relation-save.json"
echo -e "\n"

echo "[15] semantic relation update"
curl -X PUT "$BASE_URL/api/timeseries/semantic/relations" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/relation-save.json"
echo -e "\n"

echo "[16] semantic relation status update"
curl -X PATCH "$BASE_URL/api/timeseries/semantic/relations/status" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/relation-status.json"
echo -e "\n"

echo "[17] anomaly task create"
curl -X POST "$BASE_URL/api/timeseries/anomaly-tasks" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/anomaly-task-save.json"
echo -e "\n"

echo "[18] anomaly task update"
curl -X PUT "$BASE_URL/api/timeseries/anomaly-tasks" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/anomaly-task-save.json"
echo -e "\n"

echo "[19] anomaly task list query"
curl -X POST "$BASE_URL/api/timeseries/anomaly-tasks/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/anomaly-task-query.json"
echo -e "\n"

echo "[20] anomaly task status update"
curl -X PATCH "$BASE_URL/api/timeseries/anomaly-tasks/status" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/anomaly-task-status.json"
echo -e "\n"

echo "[21] forecast task create"
curl -X POST "$BASE_URL/api/timeseries/forecast-tasks" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/forecast-task-save.json"
echo -e "\n"

echo "[22] forecast task update"
curl -X PUT "$BASE_URL/api/timeseries/forecast-tasks" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/forecast-task-save.json"
echo -e "\n"

echo "[23] forecast task list query"
curl -X POST "$BASE_URL/api/timeseries/forecast-tasks/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/forecast-task-query.json"
echo -e "\n"

echo "[24] forecast task status update"
curl -X PATCH "$BASE_URL/api/timeseries/forecast-tasks/status" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/forecast-task-status.json"
echo -e "\n"

echo "[25] event list query"
curl -X POST "$BASE_URL/api/timeseries/events/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/event-query.json"
echo -e "\n"

echo "[26] event detail query"
curl -X POST "$BASE_URL/api/timeseries/events/detail" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/event-detail.json"
echo -e "\n"

echo "[27] event create"
curl -X POST "$BASE_URL/api/timeseries/events" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/event-save.json"
echo -e "\n"

echo "[28] event update"
curl -X PUT "$BASE_URL/api/timeseries/events" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/event-save.json"
echo -e "\n"

echo "[29] diagnosis query"
curl -X POST "$BASE_URL/api/timeseries/decision/diagnosis" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/decision-query.json"
echo -e "\n"

echo "[30] decision suggestion query"
curl -X POST "$BASE_URL/api/timeseries/decision/suggestion" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/decision-query.json"
echo -e "\n"

echo "[31] disposal feedback submit"
curl -X PATCH "$BASE_URL/api/timeseries/decision/feedback" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/feedback.json"
echo -e "\n"

echo "[32] anomaly result query"
curl -X POST "$BASE_URL/api/timeseries/anomaly-results/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/anomaly-result-query.json"
echo -e "\n"

echo "[33] forecast result query"
curl -X POST "$BASE_URL/api/timeseries/forecast-results/query" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/forecast-result-query.json"
echo -e "\n"

echo "[34] timeseries data ingest"
curl -X POST "$BASE_URL/api/timeseries/data/ingest" -H "Content-Type: application/json" --data-binary "@$REQ_DIR/timeseries-data-save.json"
echo -e "\n"
