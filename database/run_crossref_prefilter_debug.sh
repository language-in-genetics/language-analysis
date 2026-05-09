#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/home/languageingenetics/Word-Frequency-Analysis-}"
DUMP_DIR="${DUMP_DIR:-/dbtemp/March 2026 Public Data File from Crossref}"
RUN_ID="${RUN_ID:-$(date +%Y%m%dT%H%M%S)}"
WORK_ROOT="${WORK_ROOT:-/home/languageingenetics/crossref-prefilter-debug-$RUN_ID}"
PIPELOG="${PIPELOG:-$PROJECT_DIR/logs/crossref-prefilter-pipeline-debug-$RUN_ID.log}"
LAUNCH_LOG="${LAUNCH_LOG:-$PROJECT_DIR/logs/crossref-prefilter-debug-launch-$RUN_ID.out}"
DEBUG_DIR="${DEBUG_DIR:-$WORK_ROOT/debug}"
MONITOR_INTERVAL="${MONITOR_INTERVAL:-5}"
DEBUG_STATS_INTERVAL="${DEBUG_STATS_INTERVAL:-100000}"

cd "$PROJECT_DIR"
mkdir -p "$WORK_ROOT" "$DEBUG_DIR" logs

{
  echo "$(date -Is) starting debug Crossref prefilter run"
  echo "run_id=$RUN_ID"
  echo "project_dir=$PROJECT_DIR"
  echo "dump_dir=$DUMP_DIR"
  echo "work_root=$WORK_ROOT"
  echo "pipe_log=$PIPELOG"
  echo "debug_dir=$DEBUG_DIR"
  echo "monitor_interval=$MONITOR_INTERVAL"
  echo "debug_stats_interval=$DEBUG_STATS_INTERVAL"
  echo "run_import=${RUN_IMPORT:-0}"
} > "$LAUNCH_LOG"

env \
  PROJECT_DIR="$PROJECT_DIR" \
  DUMP_DIR="$DUMP_DIR" \
  WORK_ROOT="$WORK_ROOT" \
  PIPELOG="$PIPELOG" \
  DEBUG_DIR="$DEBUG_DIR" \
  DEBUG_STATS=1 \
  DEBUG_STATS_INTERVAL="$DEBUG_STATS_INTERVAL" \
  DEBUG_STATS_SYNC="${DEBUG_STATS_SYNC:-1}" \
  RUN_IMPORT="${RUN_IMPORT:-0}" \
  BUILD_CACHE="${BUILD_CACHE:-1}" \
  WORKERS="${WORKERS:-8}" \
  RUN_LABEL="${RUN_LABEL:-crossref-2026-annual-prefiltered-debug-$RUN_ID}" \
  /usr/bin/ionice -c2 -n7 /usr/bin/nice -n 10 \
  bash database/run_crossref_prefilter_pipeline.sh >> "$LAUNCH_LOG" 2>&1 &
pipeline_pid=$!

env \
  WORK_ROOT="$WORK_ROOT" \
  DEBUG_DIR="$DEBUG_DIR" \
  PIPELOG="$PIPELOG" \
  WRAPPER_PID="$pipeline_pid" \
  INTERVAL="$MONITOR_INTERVAL" \
  PGDATABASE="${PGDATABASE:-crossref}" \
  CACHE_PATH="${CACHE_PATH:-$WORK_ROOT/current-doi-cache.sqlite}" \
  bash database/monitor_crossref_prefilter_debug.sh >> "$LAUNCH_LOG" 2>&1 &
monitor_pid=$!

{
  echo "pipeline_pid=$pipeline_pid"
  echo "monitor_pid=$monitor_pid"
} >> "$LAUNCH_LOG"

set +e
wait "$pipeline_pid"
status=$?
set -e

if kill -0 "$monitor_pid" 2>/dev/null; then
  kill "$monitor_pid" 2>/dev/null || true
  wait "$monitor_pid" 2>/dev/null || true
fi

echo "$(date -Is) debug Crossref prefilter run exited status=$status" >> "$LAUNCH_LOG"
exit "$status"
