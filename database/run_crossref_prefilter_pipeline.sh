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
RUN_IMPORT="${RUN_IMPORT:-0}"
INCLUDE_UNKNOWN="${INCLUDE_UNKNOWN:-0}"

cd "$PROJECT_DIR"

export PGDATABASE="${PGDATABASE:-crossref}"
export PGHOST="${PGHOST:-/var/run/postgresql}"

mkdir -p logs "$WORK_ROOT"

PIPELOG="${PIPELOG:-logs/crossref-prefilter-pipeline.log}"
CACHE_MANIFEST="${CACHE_MANIFEST:-$CACHE_PATH.manifest.json}"
CLASSIFY_DIR="${CLASSIFY_DIR:-$WORK_ROOT/classified}"
IMPORT_DIR="${IMPORT_DIR:-$WORK_ROOT/importable}"

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
  echo "run_import=$RUN_IMPORT"
  echo "include_unknown=$INCLUDE_UNKNOWN"
} > "$PIPELOG"

if [[ "$BUILD_CACHE" == "1" || ! -s "$CACHE_PATH" ]]; then
  rm -f "$CACHE_PATH" "$CACHE_MANIFEST"
  ./bin/crossrefcachebuild \
    -format sqlite \
    -compute-missing-fingerprints \
    -out "$CACHE_PATH" \
    -manifest "$CACHE_MANIFEST" \
    -report-every 1000000 \
    >> "$PIPELOG" 2>&1
else
  echo "$(date -Is) reusing existing cache $CACHE_PATH" >> "$PIPELOG"
fi

rm -rf "$CLASSIFY_DIR" "$IMPORT_DIR"
mkdir -p "$CLASSIFY_DIR" "$IMPORT_DIR"

./bin/crossrefclassify \
  -cache "$CACHE_PATH" \
  -cache-format sqlite \
  -sqlite-copy-to-memory \
  -dir "$DUMP_DIR" \
  -out-dir "$CLASSIFY_DIR" \
  -workers "$WORKERS" \
  -progress-every 250 \
  >> "$PIPELOG" 2>&1

link_if_present() {
  local src="$1"
  local dst="$2"
  if [[ -s "$src" ]]; then
    ln -sf "$src" "$dst"
    echo "$(date -Is) queued $(basename "$src") for import" >> "$PIPELOG"
  fi
}

link_if_present "$CLASSIFY_DIR/new.jsonl.gz" "$IMPORT_DIR/000-new.jsonl.gz"
link_if_present "$CLASSIFY_DIR/changed.jsonl.gz" "$IMPORT_DIR/001-changed.jsonl.gz"
if [[ "$INCLUDE_UNKNOWN" == "1" ]]; then
  link_if_present "$CLASSIFY_DIR/unknown-fingerprint.jsonl.gz" "$IMPORT_DIR/002-unknown-fingerprint.jsonl.gz"
fi

if ! compgen -G "$IMPORT_DIR/*.jsonl.gz" > /dev/null; then
  echo "$(date -Is) no importable records found; stopping before import" >> "$PIPELOG"
  exit 0
fi

if [[ "$RUN_IMPORT" == "1" ]]; then
  ./bin/crossrefimport \
    -run-label "$RUN_LABEL" \
    -dir "$IMPORT_DIR" \
    -batch-size "$BATCH_SIZE" \
    >> "$PIPELOG" 2>&1
  echo "$(date -Is) Crossref prefilter import complete" >> "$PIPELOG"
else
  {
    echo "$(date -Is) classification complete; import not started because RUN_IMPORT=0"
    echo "To import:"
    echo "RUN_IMPORT=1 RUN_LABEL=$RUN_LABEL $0"
  } >> "$PIPELOG"
fi
