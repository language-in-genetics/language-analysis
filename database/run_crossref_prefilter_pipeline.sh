#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/home/languageingenetics/Word-Frequency-Analysis-}"
DUMP_DIR="${DUMP_DIR:-/dbtemp/March 2026 Public Data File from Crossref}"
WORK_ROOT="${WORK_ROOT:-$DUMP_DIR/_prefiltered_full_import}"
CACHE_PATH="${CACHE_PATH:-$WORK_ROOT/current-doi-cache.sqlite}"
RUN_LABEL="${RUN_LABEL:-crossref-2026-annual-prefiltered-$(date +%Y%m%d)}"
WORKERS="${WORKERS:-8}"
BATCH_SIZE="${BATCH_SIZE:-50000}"
BUILD_CACHE="${BUILD_CACHE:-1}"
RUN_CLASSIFY="${RUN_CLASSIFY:-1}"
RUN_IMPORT="${RUN_IMPORT:-0}"
INCLUDE_UNKNOWN="${INCLUDE_UNKNOWN:-0}"

cd "$PROJECT_DIR"

export PGDATABASE="${PGDATABASE:-crossref}"
export PGHOST="${PGHOST:-/var/run/postgresql}"

mkdir -p logs "$WORK_ROOT"

PIPELOG="${PIPELOG:-logs/crossref-prefilter-pipeline.log}"
CACHE_MANIFEST="${CACHE_MANIFEST:-$CACHE_PATH.manifest.json}"
CLASSIFY_DIR="${CLASSIFY_DIR:-$WORK_ROOT/classified}"
CLASSIFY_DB="${CLASSIFY_DB:-$CLASSIFY_DIR/classified.sqlite}"
DEBUG_DIR="${DEBUG_DIR:-$WORK_ROOT/debug}"
DEBUG_STATS="${DEBUG_STATS:-0}"
DEBUG_STATS_INTERVAL="${DEBUG_STATS_INTERVAL:-100000}"
DEBUG_STATS_SYNC="${DEBUG_STATS_SYNC:-1}"

{
  echo "$(date -Is) starting Crossref prefilter pipeline"
  echo "project_dir=$PROJECT_DIR"
  echo "dump_dir=$DUMP_DIR"
  echo "work_root=$WORK_ROOT"
  echo "cache_path=$CACHE_PATH"
  echo "run_label=$RUN_LABEL"
  echo "workers=$WORKERS"
  echo "batch_size=$BATCH_SIZE"
  echo "build_cache=$BUILD_CACHE"
  echo "run_classify=$RUN_CLASSIFY"
  echo "run_import=$RUN_IMPORT"
  echo "include_unknown=$INCLUDE_UNKNOWN"
  echo "classify_db=$CLASSIFY_DB"
  echo "debug_stats=$DEBUG_STATS"
  echo "debug_dir=$DEBUG_DIR"
} > "$PIPELOG"

if [[ "$BUILD_CACHE" == "1" || ! -s "$CACHE_PATH" ]]; then
  rm -f "$CACHE_PATH" "$CACHE_MANIFEST"
  cachebuild_args=(
    -format sqlite \
    -compute-missing-fingerprints \
    -out "$CACHE_PATH" \
    -manifest "$CACHE_MANIFEST" \
    -report-every 1000000
  )
  if [[ "$DEBUG_STATS" == "1" ]]; then
    mkdir -p "$DEBUG_DIR"
    cachebuild_args+=(
      -debug-stats "$DEBUG_DIR/cachebuild-stats.jsonl"
      -debug-stats-every "$DEBUG_STATS_INTERVAL"
    )
    if [[ "$DEBUG_STATS_SYNC" == "1" ]]; then
      cachebuild_args+=(-debug-stats-sync)
    fi
    echo "$(date -Is) writing cache builder debug stats to $DEBUG_DIR/cachebuild-stats.jsonl every $DEBUG_STATS_INTERVAL rows" >> "$PIPELOG"
  fi
  ./bin/crossrefcachebuild "${cachebuild_args[@]}" >> "$PIPELOG" 2>&1
else
  echo "$(date -Is) reusing existing cache $CACHE_PATH" >> "$PIPELOG"
fi

if [[ "$RUN_CLASSIFY" == "1" ]]; then
  rm -rf "$CLASSIFY_DIR"
  mkdir -p "$CLASSIFY_DIR"

  ./bin/crossrefclassify \
    -cache "$CACHE_PATH" \
    -cache-format sqlite \
    -sqlite-copy-to-memory \
    -dir "$DUMP_DIR" \
    -out-dir "$CLASSIFY_DIR" \
    -sqlite-out "$CLASSIFY_DB" \
    -workers "$WORKERS" \
    -progress-every 250 \
    >> "$PIPELOG" 2>&1
else
  if [[ ! -s "$CLASSIFY_DB" ]]; then
    echo "$(date -Is) RUN_CLASSIFY=0 but SQLite stage is missing: $CLASSIFY_DB" >> "$PIPELOG"
    exit 1
  fi
  echo "$(date -Is) reusing existing SQLite stage $CLASSIFY_DB" >> "$PIPELOG"
fi

IMPORT_CATEGORIES="new,changed"
if [[ "$INCLUDE_UNKNOWN" == "1" ]]; then
  IMPORT_CATEGORIES="new,changed,unknown-fingerprint"
fi

if [[ "$RUN_IMPORT" == "1" ]]; then
  ./bin/crossrefimport \
    -run-label "$RUN_LABEL" \
    -sqlite "$CLASSIFY_DB" \
    -categories "$IMPORT_CATEGORIES" \
    -batch-size "$BATCH_SIZE" \
    >> "$PIPELOG" 2>&1
  echo "$(date -Is) Crossref prefilter import complete" >> "$PIPELOG"
else
  {
    echo "$(date -Is) classification complete; import not started because RUN_IMPORT=0"
    echo "SQLite stage: $CLASSIFY_DB"
    echo "To import:"
    echo "BUILD_CACHE=0 RUN_CLASSIFY=0 RUN_IMPORT=1 CLASSIFY_DB=$CLASSIFY_DB RUN_LABEL=$RUN_LABEL $0"
  } >> "$PIPELOG"
fi
