@echo off
setlocal enabledelayedexpansion
set "BASE_URL=http://localhost:8080"
set "REQ_DIR=%~dp0src\test\resources\command-requests\ett"

echo ============================================
echo   ETTh1 Full Flow Test
echo ============================================
echo.

echo --- Step 1: Create Category ---
curl -s -X POST "%BASE_URL%/api/timeseries/semantic/categories" ^
  -H "Content-Type: application/json" ^
  --data-binary "@%REQ_DIR%\ett-category.json"
echo.

echo --- Step 2: Register 7 Sequence Instances ---
for %%s in (HUFL HULL MUFL MULL LUFL LULL OT) do (
  curl -s -X POST "%BASE_URL%/api/timeseries/instances" ^
    -H "Content-Type: application/json" ^
    --data-binary "@%REQ_DIR%\ett-instance-%%s.json"
  echo %%s
)
echo.

echo --- Step 3: Create Constraint (OT < 40) ---
curl -s -X POST "%BASE_URL%/api/timeseries/semantic/constraints" ^
  -H "Content-Type: application/json" ^
  --data-binary "@%REQ_DIR%\ett-constraint.json"
echo.

echo --- Step 4: Ingest First 3 Hours of Data ---
curl -s -X POST "%BASE_URL%/api/timeseries/data/ingest" ^
  -H "Content-Type: application/json" ^
  --data-binary "@%REQ_DIR%\ett-data-ingest.json"
echo.

echo --- Step 5: Create Anomaly Task ---
curl -s -X POST "%BASE_URL%/api/timeseries/anomaly-tasks" ^
  -H "Content-Type: application/json" ^
  --data-binary "@%REQ_DIR%\ett-anomaly-task.json"
echo.

echo --- Step 6: Query Anomaly Results ---
curl -s -X POST "%BASE_URL%/api/timeseries/anomaly-results/query" ^
  -H "Content-Type: application/json" ^
  -d "{\"taskId\":\"ett-anomaly-001\",\"sequenceId\":\"ETTh1_OT\",\"startTime\":\"2016-07-01T00:00:00\",\"endTime\":\"2016-07-02T00:00:00\"}"
echo.

echo.
echo ============================================
echo   Done! Check data/ folder for persisted files.
echo ============================================
