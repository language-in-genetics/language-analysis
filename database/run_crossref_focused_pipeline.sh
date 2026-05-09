#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/home/languageingenetics/Word-Frequency-Analysis-}"
DUMP_DIR="${DUMP_DIR:-/dbtemp/March 2026 Public Data File from Crossref}"
FOCUS_DIR="${FOCUS_DIR:-$DUMP_DIR/_focused_journals_doi}"
NODOI_DIR="${NODOI_DIR:-$DUMP_DIR/_focused_journals_no_doi}"
RUN_LABEL="${RUN_LABEL:-crossref-2026-focused-journals-20260505}"
WORKERS="${WORKERS:-8}"
BATCH_SIZE="${BATCH_SIZE:-5000}"

cd "$PROJECT_DIR"

export PGDATABASE="${PGDATABASE:-crossref}"
export PGHOST="${PGHOST:-/var/run/postgresql}"

mkdir -p logs "$FOCUS_DIR" "$NODOI_DIR"

JOURNALS="logs/crossref-focused-journals.txt"
PIPELOG="logs/crossref-focused-pipeline.log"
FOCUSED_SQLITE="${FOCUSED_SQLITE:-$FOCUS_DIR/focused-journals.sqlite}"

{
  echo "$(date -Is) starting focused Crossref pipeline"
  echo "project_dir=$PROJECT_DIR"
  echo "dump_dir=$DUMP_DIR"
  echo "focus_dir=$FOCUS_DIR"
  echo "focused_sqlite=$FOCUSED_SQLITE"
  echo "run_label=$RUN_LABEL"
  echo "workers=$WORKERS"
  echo "batch_size=$BATCH_SIZE"
} > "$PIPELOG"

psql -At -c "SELECT name FROM languageingenetics.journals WHERE enabled ORDER BY name" > "$JOURNALS"
echo "$(date -Is) journals=$(wc -l < "$JOURNALS")" >> "$PIPELOG"

rm -f "$FOCUSED_SQLITE" "$FOCUSED_SQLITE.tmp"

./bin/crossreffilter \
  -dir "$DUMP_DIR" \
  -journals "$JOURNALS" \
  -sqlite-out "$FOCUSED_SQLITE" \
  -require-doi \
  -workers "$WORKERS" \
  -progress-every 250 \
  >> "$PIPELOG" 2>&1

echo "$(date -Is) focused filter complete; starting DOI-only import" >> "$PIPELOG"

./bin/crossrefimport \
  -run-label "$RUN_LABEL" \
  -sqlite "$FOCUSED_SQLITE" \
  -categories focused \
  -batch-size "$BATCH_SIZE" \
  >> "$PIPELOG" 2>&1

echo "$(date -Is) focused pipeline complete" >> "$PIPELOG"
